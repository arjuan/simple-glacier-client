package arjuan;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.event.ProgressListener;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.UploadResult;


public class ArchiveUploadHighLevel {
    
	private String region     	= null;
	private String account		= null;
	private String vault		= null;
	private String description	= null;
	
	private final Log log = LogFactory.getLog(ArchiveUploadHighLevel.class);

    // Ctor
	public ArchiveUploadHighLevel(String region, String account, String vault) {
        this.region  = region;
        this.account = account;
		this.vault   = vault;
	}
	
	/**
	 * Upload an archive (assuming full or relative path) to AWS Glacier
	 */
	public void upload(String archive) throws IOException {
		this.upload(archive, null, null);
	}
	
	/**
	 * Upload an archive in the given dir to an AWS Glacier vault and assign it with given description
	 */
	public void upload(String archive, String dir, String description) throws IOException {
	
		// load the credentials from the .aws profile
    	ProfileCredentialsProvider credentials = new ProfileCredentialsProvider();
    	
        AmazonGlacierClient client = new AmazonGlacierClient(credentials);
		
		// Uploading to Glacier at Ireland (eu-west-1)
		client.setEndpoint("https://glacier." + this.region + ".amazonaws.com/");

		// if given, concatenate dir to archive name
		String fileName = (dir == null) ? archive : dir + File.separatorChar + archive;
		
		// handle null description
		if (description == null) {
			description = archive + " on " + (new Date());
		}

		log.info("File (archive): " + fileName);
		log.info("Description: " + description);

		try {
			File file = new File(fileName);
			ArchiveTransferManager atm = new ArchiveTransferManager(client, credentials);
			UploadResult result = atm.upload(account, vault, description, file , new UploadProgressListener(file.length()));
			
			// Good news! 
			log.info("Done! Archive ID: " + result.getArchiveId());

		} catch (Exception e) {
			log.error(e);
		}
	}
	
	
	/**
	 * PRIVATE CLASS LISTENER FOR PROGRESS EVENTS
	 */
	private class UploadProgressListener implements ProgressListener {

		private long total      = 0L;
		private long counter    = 0L;
		private long partSize   = 0L;

		UploadProgressListener(long total) {
			this.total = total;
		}

		@Override
		public void progressChanged(ProgressEvent progressEvent) {

			if (progressEvent.getEventType() == ProgressEventType.REQUEST_CONTENT_LENGTH_EVENT) {
				partSize = progressEvent.getBytes();
				ArchiveUploadHighLevel.this.log.info(new Date() + "\t" + "Part size: " + partSize);
            }

			if (progressEvent.getEventType() == ProgressEventType.CLIENT_REQUEST_SUCCESS_EVENT) {
				counter += partSize;
				int percentage = (int)(counter * 100.0 / total);
				ArchiveUploadHighLevel.this.log.info(new Date() + "\t" + "Successfully transferred: " + counter + " / " + total + " (" + percentage + "%)");
			}
		}
	}
}

