package arjuan;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.glacier.AmazonGlacierClient;

public abstract class BaseAmazonGlacierClientAware {
    
    protected String region                 = null;
    protected String account                = null;
    protected String vault                  = null;
    
    protected AmazonGlacierClient        awsClient           = null;
    protected ProfileCredentialsProvider credentialsProvider = null;
    
    // Ctor
    protected BaseAmazonGlacierClientAware(String region, String account, String vault) {
        
        this.region  = region;
        this.account = account;
        this.vault   = vault;

        //@TODO Need to move away from deprecated fashion of acquiring a client
        //@TODO Also, consider supporting other ways of passing credentials (other than relying of local .aws profile)
        
        // load the credentials from the .aws profile
        this.credentialsProvider = new ProfileCredentialsProvider();
        
        this.awsClient = new AmazonGlacierClient(this.credentialsProvider);
        
        // Set Glacier end-point
        this.awsClient.setEndpoint("https://glacier." + this.region + ".amazonaws.com/");
    }
    
    protected String getRegion() {
        return this.region;
    }

    protected String getAccount() {
        return this.account;
    }

    protected String getVault() {
        return this.vault;
    }

}

