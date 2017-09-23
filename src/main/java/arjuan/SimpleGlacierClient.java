package arjuan;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;

import com.amazonaws.event.ProgressListener;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.UploadResult;


public class SimpleGlacierClient {
    
    static String region         = null;
    static String account        = null;
    static String vault          = null;
    static String archive        = null;
    static String dir            = null;
    static String description    = null;
    
    static final Log     log     = LogFactory.getLog(SimpleGlacierClient.class);
    static final Options options = new Options();


    // static initializer
    static {
		
        // commands
		OptionGroup commands = new OptionGroup();
		commands.addOption(Option.builder("upload").required().desc("Upload an archive file to AWS Glacier").build());
		commands.addOption(Option.builder("list").required().desc("List all archives in an AWS Glacier vault").build());
		options.addOptionGroup(commands);
		
		// option flags
		options.addOption(Option.builder("r").longOpt("region").required().hasArg().desc("The AWS region").build());
        options.addOption(Option.builder("a").longOpt("account").required().hasArg().desc("The AWS account id").build());
        options.addOption(Option.builder("v").longOpt("vault").required().hasArg().desc("The AWS Glacier vault name").build());
        options.addOption(Option.builder("f").longOpt("file").required().hasArg().desc("The name of the archive file to upload to AWS").build());
        options.addOption(Option.builder("p").longOpt("path").hasArg().desc("A path prefix to the local directory containing the archive file").build());
        options.addOption(Option.builder("d").longOpt("description").required().hasArg().desc("A description string for the archive file upload").build());
        options.addOption(Option.builder("h").longOpt("help").desc("Print this usage message").build());
    }

    /**
     * Main method
     */
    public static void main(String[] args) throws IOException {
    
         // handle -h option
        parseForHelp(args);
        
        // parse command line for all other (non-help) cli options
        CommandLine cli = null;
        try {
            cli = new DefaultParser().parse(options, args, false);
        } catch (ParseException pe) {
            System.err.println(pe.getMessage());
            System.exit(-1);
        }
    
        // handle execution options
        region      = cli.getOptionValue("r");
        account     = cli.getOptionValue("a");
        vault       = cli.getOptionValue("v");
        archive     = cli.getOptionValue("f");
        dir         = cli.getOptionValue("p");
        description = cli.getOptionValue("d");

		log.info("Region: " + region);
		log.info("Account: " + account);
		log.info("Vault: " + vault);

		if (cli.hasOption("upload")) {
			new ArchiveUploadHighLevel(region, account, vault).upload(archive, dir, description);
			
		} else {
			throw new UnsupportedOperationException("Currently only the 'upload' command is supported");
		}
	}
    
    static void parseForHelp(String[] args) {
        
        // parse for help options
        try {
            Options helpOnly = new Options();
            helpOnly.addOption(options.getOption("h"));
            CommandLine cli = new DefaultParser().parse(helpOnly, args, true);
            
            // no -h option - do nothing
            if (!cli.hasOption("h")) {
                return;
            }
        } catch (ParseException pe) {
            System.err.println(pe.getMessage());
            System.exit(-1);
        }
    
        // print help text
        new HelpFormatter().printHelp("sgc", "Simple Glacier Client (sgc) | Version: 0.1\n\n", options, "\n"+"See: https://github.com/arjuan/simple-glacier-client"+"\n\n", true);
    }
}

