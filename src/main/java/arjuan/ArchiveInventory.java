package arjuan;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.event.ProgressListener;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.UploadResult;
import com.amazonaws.services.glacier.model.JobParameters;
import com.amazonaws.services.glacier.model.InitiateJobRequest;
import com.amazonaws.services.glacier.model.InitiateJobResult;
import com.amazonaws.services.glacier.model.ListJobsRequest;
import com.amazonaws.services.glacier.model.ListJobsResult;
import com.amazonaws.services.glacier.model.GetJobOutputRequest;
import com.amazonaws.services.glacier.model.GetJobOutputResult;
import com.amazonaws.services.glacier.model.GlacierJobDescription;



public class ArchiveInventory extends BaseAmazonGlacierClientAware {
    
    private final Log log = LogFactory.getLog(ArchiveInventory.class);

    // Ctor
    public ArchiveInventory(String region, String account, String vault) {
        super (region, account, vault);
    }
        
    /**
     * Upload an archive in the given dir to an AWS Glacier vault and assign it with given description
     */
    public void list() throws IOException {
        this.list ("CSV", 15, null, null, null);
    }

    /**
     * retrieve the list of archives in this objects vault
     */
    public void list(String format, int interval, String description, String jobId, String fileName) throws IOException {
    
        log.debug("Listing archives in " + format + " format");

        if (jobId == null) {
            
            // try to find the latest job of the given format and who's results are still available (AWS keeps results for 24 hours)
            GlacierJobDescription potentialJob = retreiveLatestJobResults(format);
            if (potentialJob != null) {
                jobId = potentialJob.getJobId();
                log.info("Found job with Id "+ jobId + " which was successfuly completed on " + potentialJob.getCompletionDate());
            } else {
                // no job id given and no recent job results are available -> submit a new retrival job and wait for it to complete
                jobId = sendInventoryRetrievalJobRequest(format, description, interval);
                log.info("A new job request with Id "+ jobId + " is now completed");
            } 
        } else {
                // provide some information in the log..
                log.info("Looking specifically for the output of job with Id " + jobId);
        }

        // job is completed - request output and log it line by line
        GetJobOutputResult jobOutput = this.awsClient.getJobOutput(new GetJobOutputRequest().withAccountId(this.account).withVaultName(this.vault).withJobId(jobId));
        
        // determine target file name using the given name or use default name if needed
        String path = fileName;
        if (path == null) {
            
            // caclulate file name suffix out of the JOB content type (either CSV or JSON)
            String suffix = jobOutput.getContentType();
            if (suffix.equalsIgnoreCase("text/csv")) {
                suffix = "csv";
            } else if (suffix.equalsIgnoreCase("application/json")) {
                suffix = "json";
            } else {
                suffix = format; // not supposed to get here...
            }
            
            // default - informative - file output file name
            path = "aws.glacier." + this.region + "." + this.vault + "." + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "." + suffix;
        }

        // write to file
        log.info("Writing job output in a file named " + path);
        Files.copy(jobOutput.getBody(), Paths.get(path), StandardCopyOption.REPLACE_EXISTING);
        
        this.awsClient.shutdown();

        this.log.info("Done");
    }
    
    private GlacierJobDescription retreiveJobResultByJobId(String jobId) {
        
        this.log.info("Looking for specific job by Job Id: " + jobId);

        // Prepare a job list request to figure out the status of the job
        ListJobsRequest listJobsRequest = new ListJobsRequest().withAccountId(this.account).withVaultName(this.vault).withCompleted("true").withStatuscode("Succeeded");
        
        for (GlacierJobDescription job : this.awsClient.listJobs(listJobsRequest).getJobList()){
            
            // found our job?
            if (job.getJobId().equals(jobId)) {
                return job;
            }
        }
            
        // job not found
        return null;
    }
    
    private GlacierJobDescription retreiveLatestJobResults(String format) {
        
        this.log.info("Looking for recently successfully completed jobs of format: " + format);

        GlacierJobDescription latest = null;
        Date latestCompletionDate    = null;

        // set the DateFormat to be able to parse and compare AWS jlacier job completion dates
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        
        // Traverse a job list that potentially matches the request to list archives in the given format
        ListJobsRequest listJobsRequest = new ListJobsRequest().withAccountId(this.account).withVaultName(this.vault).withCompleted("true").withStatuscode("Succeeded");
        for (GlacierJobDescription job : this.awsClient.listJobs(listJobsRequest).getJobList()) {
            
            // skip irrelevant job types
            if (! job.getAction().equalsIgnoreCase("InventoryRetrieval") ) continue;
            
            // skip irrelevant formats
            if (! format.equalsIgnoreCase(job.getInventoryRetrievalParameters().getFormat()) ) continue;
            
            Date currentCompletionDate = null;
            try {
                currentCompletionDate = dateFormat.parse(job.getCompletionDate());
            } catch (Exception e) {
                log.warn(e);
                
                // no point in continue checking more jobs given that we have the date format wrong :(
                break;
            }
            
            // compare against previously found jobs (if any) to pick that latest
            if (latestCompletionDate == null || currentCompletionDate.after(latestCompletionDate)) {
                latest = job;
                latestCompletionDate = currentCompletionDate;
            }
        }
            
        // job not found
        return latest;
    }
    
    private String sendInventoryRetrievalJobRequest(String format, String description, int interval) {
        
        log.info("Description: " + description);
        log.info("Interval: " + interval);

        // craete an inventory retrival request
        JobParameters params = new JobParameters().withType("inventory-retrieval").withFormat(format).withDescription(description);
        InitiateJobRequest request = new InitiateJobRequest(account, vault, params);
        
        // Initiate job and start polling for its completion
        InitiateJobResult initiateJobResult = this.awsClient.initiateJob(request);

        String jobId = initiateJobResult.getJobId();
        this.log.info("Initiated job 'inventory-retrieval'. jobId=" + jobId);
                    
        // wait for the job to complete
        GlacierJobDescription jobDescription = retreiveJobResultByJobId(jobId);
        while (jobDescription == null || ! jobDescription.isCompleted()) {
            
            // log
            this.log.info("Job not completed! Will try again in " + interval + " minutes");
            
            // wait before accessing again (remember: AWS are counting API calls!!)
            try {
                TimeUnit.MINUTES.sleep(interval);
            } catch (InterruptedException ie) {}
            
            // re-check job status
            jobDescription = retreiveJobResultByJobId(jobId);
        }

        return jobId;
    }
}

