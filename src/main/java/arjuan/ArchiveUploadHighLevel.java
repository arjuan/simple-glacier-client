package glacier;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import com.amazonaws.event.ProgressListener;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.UploadResult;


public class ArchiveUploadHighLevel {
    
	public static String region     	= null;
	public static String account		= null;
	public static String vault		= null;
    	public static String archive		= null;
    	public static String dir		= null;
    	public static String description	= null;
	    
        public static void main(String[] args) throws IOException {
		
                // Initialize all needed values per the system properties
		init();
		
		// load the credentials from the .aws profile
    	        ProfileCredentialsProvider credentials = new ProfileCredentialsProvider();
    	
                AmazonGlacierClient client = new AmazonGlacierClient(credentials);
		
		// Uploading to Glacier at Ireland (eu-west-1)
                client.setEndpoint("https://glacier." + region + ".amazonaws.com/");

                // if given, concatenate dir to archive name
                String fileName = (dir == null) ? archive : dir + File.separatorChar + archive;

                System.out.println("File (archive)      : " + fileName);
                System.out.println("Description         : " + description);
                System.out.println("Region              : " + region);
                System.out.println("Account             : " + account);
                System.out.println("Vault               : " + vault);
	
                try {
                        File file = new File(fileName);
                        ArchiveTransferManager atm = new ArchiveTransferManager(client, credentials);
                        UploadResult result = atm.upload(account, vault, description, file , new UploadProgressListener(file.length()));
                        
                        System.out.println(new Date() + "\t" + "Done! Archive ID: " + result.getArchiveId());
                    
                } catch (Exception e) {
                    System.err.println(e);
                }
        }
	
	private static void init() {

		region = System.getProperty("region");
		if (region == null || region.trim().length() == 0 || region.equals("${region}")) {
                        throw new RuntimeException("System property 'region' is not set. Use -Dregion=<region name>");
		}

		account = System.getProperty("account");
		if (account == null || account.trim().length() == 0 || account.equals("${account}")) {
			throw new RuntimeException("System property 'account' is not set. Use -Daccount=<account id>");
		}
 
		vault = System.getProperty("vault");
		if (vault == null || vault.trim().length() == 0 || vault.equals("${vault}")) {
			throw new RuntimeException("System property 'vault' is not set. Use -Dvault=<vault>");
		}
        
		archive	= System.getProperty("archive");
		if (archive == null || archive.trim().length() == 0 || archive.trim().equals("${archive}")) {
			throw new RuntimeException("System property 'archive' is not set. Use -Darchive=<archive>");
		}
		
		dir = System.getProperty("dir");
		if (dir != null && (dir.trim().length() == 0 || dir.trim().equals("${dir}"))) {
			dir = null;
		}

		description = System.getProperty("description");
		if (description == null || description.trim().length() == 0 || description.trim().equals("${description}")) {
			description = archive + " on " + (new Date());
		}
	}


	private static class UploadProgressListener implements ProgressListener {

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
                                System.out.println(new Date() + "\t" + "Part size: " + partSize);
                        }

			if (progressEvent.getEventType() == ProgressEventType.CLIENT_REQUEST_SUCCESS_EVENT) {
                                counter += partSize;
                                int percentage = (int)(counter * 100.0 / total);
			        System.out.println(new Date() + "\t" + "Successfully transferred: " + counter + " / " + total + " (" + percentage + "%)");
                        }
		}
	}
}
