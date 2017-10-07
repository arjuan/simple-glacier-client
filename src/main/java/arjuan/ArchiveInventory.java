package arjuan;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
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
        this.list ("CSV", 15, null, null);
    }

    /**
     * retrieve the list of archives in this objects vault
     */
    public void list(String format, int interval, String description, String jobId) throws IOException {
    
        log.info("Format   : " + format);
        log.info("Interval : " + interval);
        log.info("Job Id   : " + jobId);

        if (jobId == null) {
			
			// not previously requested job given, request a new job
			jobId = sendInventoryRetrievalJobRequest(format, description);
			
			// wait for the job to complete
			GlacierJobDescription jobDescription = retreiveJobResult(account, vault, jobId);
			while (jobDescription == null || ! jobDescription.isCompleted()) {
				
				// log
				this.log.info("Job not completed! Will try again in " + interval + " minutes");
				
				// wait before accessing again (remember: AWS are counting API calls!!)
				try {
					TimeUnit.MINUTES.sleep(interval);
				} catch (InterruptedException ie) {}
				
				// re-check job status
				jobDescription = retreiveJobResult(account, vault, jobId);
			}
		}
        
        // job is completed - request output and log it line by line
        GetJobOutputResult jobOutput = this.awsClient.getJobOutput(new GetJobOutputRequest().withAccountId(this.account).withVaultName(this.vault).withJobId(jobId));
        BufferedReader reader = new BufferedReader(new InputStreamReader(jobOutput.getBody()));
        String line = reader.readLine();
        while (line != null) {
            // log
            this.log.info(line);

            line = reader.readLine();
        }
        
        this.log.info("Done");
        
        this.awsClient.shutdown();
    }
	
	private String sendInventoryRetrievalJobRequest(String format, String description) {
		
        // craete an inventory retrival request
        JobParameters params = new JobParameters().withType("inventory-retrieval").withFormat(format).withDescription(description);
        InitiateJobRequest request = new InitiateJobRequest(account, vault, params);
        
        // Initiate job and start polling for its completion
        InitiateJobResult initiateJobResult = this.awsClient.initiateJob(request);
        this.log.info("Initiated job 'inventory-retrieval'. jobId=" + initiateJobResult.getJobId());

		return initiateJobResult.getJobId();
	}
    
    private GlacierJobDescription retreiveJobResult(String accountId, String vaultName, String jobId) {
        
        // Prepare a job list request to figure out the status of the job
        ListJobsRequest listJobsRequest = new ListJobsRequest().withAccountId(accountId).withVaultName(vaultName).withCompleted("true");
        
        for (GlacierJobDescription job : this.awsClient.listJobs(listJobsRequest).getJobList()){
            
            // found our job?
            if (job.getJobId().equals(jobId)) {
                return job;
            }
        }
            
        // job not found
        return null;
    }
}

