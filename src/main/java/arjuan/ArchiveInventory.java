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



public class ArchiveInventory {
    
    private String region                 = null;
    private String account                = null;
    private String vault                  = null;
	private AmazonGlacierClient awsClient = null;
    
    private final Log log = LogFactory.getLog(ArchiveInventory.class);

    // Ctor
    public ArchiveInventory(String region, String account, String vault) {
        
		this.region  = region;
        this.account = account;
        this.vault   = vault;

        // load the credentials from the .aws profile
        ProfileCredentialsProvider credentials = new ProfileCredentialsProvider();
        
        this.awsClient = new AmazonGlacierClient(credentials);
        
        // Set Glacier end-point
        this.awsClient.setEndpoint("https://glacier." + this.region + ".amazonaws.com/");
	}
        
    /**
     * Upload an archive in the given dir to an AWS Glacier vault and assign it with given description
     */
    public void list() throws IOException {
		this.list ("CSV", 10);
	}

    /**
     * Upload an archive in the given dir to an AWS Glacier vault and assign it with given description
     */
    public void list(String format, int interval) throws IOException {
    
        log.info("Format   : " + format);
        log.info("Interval : " + interval);

		// craete an inventory retrival request
		JobParameters params = new JobParameters().withType("inventory-retrieval").withFormat(format);
		InitiateJobRequest request = new InitiateJobRequest(account, vault, params);
		
		// Initiate job and start polling for its completion
		InitiateJobResult initiateJobResult = this.awsClient.initiateJob(request);
		this.log.info("Initiated job 'inventory-retrieval'. jobId=" + initiateJobResult.getJobId());
		
		GlacierJobDescription jobDescription = retreiveJobResult(account, vault, initiateJobResult.getJobId());
		while (jobDescription == null || ! jobDescription.isCompleted()) {
			
			// log
			this.log.info("Job not completed! Will try again in " + interval + " seconds");
			
			// wait before accessing again (remember: AWS are counting API calls!!)
			try {
				TimeUnit.SECONDS.sleep(interval);
			} catch (InterruptedException ie) {}
			
			// re-check job status
			jobDescription = retreiveJobResult(account, vault, initiateJobResult.getJobId());
		}
		
		// job is completed - request output and log it line by line
		GetJobOutputResult jobOutput = this.awsClient.getJobOutput(new GetJobOutputRequest().withAccountId(this.account).withVaultName(this.vault).withJobId(initiateJobResult.getJobId()));
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

