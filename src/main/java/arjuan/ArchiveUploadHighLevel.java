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

public class ArchiveUploadHighLevel extends BaseAmazonGlacierClientAware {
    
    private ArchiveTransferManager atm = null;
    
    private final Log log = LogFactory.getLog(ArchiveUploadHighLevel.class);

    // Ctor
    public ArchiveUploadHighLevel(String region, String account, String vault) {
        
        super (region, account, vault);
        
        this.atm = new ArchiveTransferManager(this.awsClient, this.credentialsProvider);
    }
    
    /**
     * Upload an archive (assuming full or relative path) to AWS Glacier
     */
    public void upload(String fileName) throws IOException {
        this.upload(fileName, null);
    }
    
    /**
     * Upload an archive in the given dir to an AWS Glacier vault and assign it with given description
     */
    public void upload(String fileName, String description) throws IOException {
    
        try {
            
            File file = new File(fileName);
            
            // handle null description
            if (description == null) {
                description = file.getName() + " on " + (new Date());
            }

            log.info("File (archive): " + fileName);
            log.info("Description: " + description);
            
            // upload
            UploadResult result = this.atm.upload(account, vault, description, file , new UploadProgressListener(file.length()));
            
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
                ArchiveUploadHighLevel.this.log.info("Part size: " + partSize);
            }

            if (progressEvent.getEventType() == ProgressEventType.CLIENT_REQUEST_SUCCESS_EVENT) {
                counter += partSize;
                int percentage = (int)(counter * 100.0 / total);
                ArchiveUploadHighLevel.this.log.info("Successfully transferred: " + counter + " / " + total + " (" + percentage + "%)");
            }
        }
    }
}

