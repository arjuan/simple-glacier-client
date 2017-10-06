package arjuan;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;

/**
 * Main class
 */
public class SimpleGlacierClient {
    
    static String region         = null;
    static String account        = null;
    static String vault          = null;
    
    static final Log     log     = LogFactory.getLog(SimpleGlacierClient.class);
    static final Options options = new Options();


    // static initializer
    static {
        
        // commands
        OptionGroup commands = new OptionGroup();
        commands.addOption(Option.builder("upload").required().desc("Upload an archive file to AWS Glacier").build());
        commands.addOption(Option.builder("list").required().desc("List all archives in an AWS Glacier vault").build());
        options.addOptionGroup(commands);
        
        // required option flags
        options.addOption(Option.builder("r").longOpt("region").required().hasArg().desc("The AWS region").build());
        options.addOption(Option.builder("a").longOpt("account").required().hasArg().desc("The AWS account id").build());
        options.addOption(Option.builder("v").longOpt("vault").required().hasArg().desc("The AWS Glacier vault name").build());
        
        // file options flags
        options.addOption(Option.builder("f").longOpt("file").hasArg().desc("File name to be used for the AWS job (either as upload or or output file)").build());
		
		// job description option
        options.addOption(Option.builder("d").longOpt("description").hasArg().desc("A description string for the archive file upload").build());
        
        // list inventory options flags
        options.addOption(Option.builder("int").longOpt("interval").hasArg().desc("An interval of time (minutes) to wait before polling for 'list' job completion").build());
        options.addOption(Option.builder("fmt").longOpt("format").hasArg().desc("Format of the 'list' result, either 'CSV' or 'json'").build());
        options.addOption(Option.builder("j").longOpt("job").hasArg().desc("Job Id of a previously requested retrival job").build());

        // help option
        options.addOption(Option.builder("h").longOpt("help").desc("Print this usage message").build());
    }

    /**
     * Main method
     */
    public static void main(String[] args) {
    
         // handle -h option
        parseForHelp(args);
        
        // parse command line for all other (non-help) cli options
        CommandLine cli = null;
        try {
            cli = new DefaultParser().parse(options, args, false);
        } catch (ParseException pe) {
            log.error(pe);
            System.exit(-100);
        }
    
        // handle execution options
        region      = cli.getOptionValue("r");
        account     = cli.getOptionValue("a");
        vault       = cli.getOptionValue("v");

        log.info("Region: " + region);
        log.info("Account: " + account);
        log.info("Vault: " + vault);

        if (cli.hasOption("upload")) {
            handleUploadCommand(cli);
        } if (cli.hasOption("list")) {
            handleListCommand(cli);
        } else {
            throw new UnsupportedOperationException("Currently only the '-upload' and '-list' commands are supported");
        }
    }
    
    private static void handleUploadCommand(CommandLine cli) {       
        String archive = cli.getOptionValue("f");
        if (archive == null || archive.trim().length() == 0) {
            throw new IllegalArgumentException("Missing archive file name. Either use the -f option properly or use -h for help");
        }
        
        // instantiate a new uploader and use it to upload the given file
        ArchiveUploadHighLevel uploader = new ArchiveUploadHighLevel(region, account, vault);
        try {
            uploader.upload(archive, cli.getOptionValue("d"));
        } catch (IOException ioe) {
            log.error(ioe);
            System.exit(-200);
        }
    }
    
    private static void handleListCommand(CommandLine cli) {
        
        // handle format and interval options...
        int interval = 15;
        if (cli.hasOption("int")) {
            interval = Integer.valueOf(cli.getOptionValue("int"));
			
			if (interval <= 0) {
				throw new IllegalArgumentException("Interval cannot be zero or negative");
			}
        }

        String format = "CSV";
        if (cli.hasOption("fmt")) {
            format = cli.getOptionValue("fmt");
        }

        String jobId = null;
        if (cli.hasOption("j")) {
            jobId = cli.getOptionValue("j");
        }
		
		String description = null;
		if (cli.hasOption("d")) {
			description = cli.getOptionValue("d");
		}

        // instantiate a new uploader and use it to upload the given file
        ArchiveInventory inventory = new ArchiveInventory(region, account, vault);
        try {
            inventory.list(format, interval, description, jobId);
        } catch (IOException ioe) {
            log.error(ioe);
            System.exit(-200);
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
        new HelpFormatter().printHelp("java -jar sgc-xxx.jar", "Simple Glacier Client (sgc) | Version: 0.1\n\n", options, "\n"+"See: https://github.com/arjuan/simple-glacier-client"+"\n\n", true);
    }
}

