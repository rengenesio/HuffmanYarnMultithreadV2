package br.ufrj.ppgi.huffmanyarnmultithreadv2.encoder.yarn;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import br.ufrj.ppgi.huffmanyarnmultithreadv2.Defines;


public class EncoderClient {
//	
	private static final Log LOG = LogFactory.getLog(EncoderClient.class);

	// YarnClient
	private YarnClient yarnClient;
	
	// File to be compressed
	private String fileName;

	// Constructor
	public EncoderClient(String fileName) throws Exception {
		this.fileName = fileName;
	}

	// Client run method
	public boolean run() throws YarnException, IOException {
		// Instantiates a configuration for this job
		Configuration configuration = new YarnConfiguration();
		
		// Initializes the yarnClient
		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(configuration);
		yarnClient.start();
		
		// Get a new application id
		YarnClientApplication app = yarnClient.createApplication();

		// Set the application context
		ApplicationSubmissionContext appicationMasterContext = app.getApplicationSubmissionContext();
		ApplicationId appId = appicationMasterContext.getApplicationId();
		appicationMasterContext.setKeepContainersAcrossApplicationAttempts(false);
		appicationMasterContext.setApplicationName(Defines.jobName);
		
		// Set up the container launch context for the application master
		ContainerLaunchContext applicationMasterContainerContext = Records.newRecord(ContainerLaunchContext.class);

		// Set up application master resource requirements, priority and queue
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(Defines.applicationMasterMemory);
		capability.setVirtualCores(Defines.applicationMasterVCores);
		appicationMasterContext.setResource(capability);
		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(Defines.applicationNasterPriority);
		appicationMasterContext.setPriority(priority);
		appicationMasterContext.setQueue(Defines.applicationMasterQueue);
				
		// Set container for context
		appicationMasterContext.setAMContainerSpec(applicationMasterContainerContext);

		// Instance of FileSystem
		FileSystem fileSystem = FileSystem.get(configuration);
		
		// Set local resources for the application master local files or archives as needed. In this scenario, the jar file for the application master is part of the local resources
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		
		// Copy the application master jar to the filesystem. Create a local resource to point to the destination jar path
//		
		LOG.info("Copying AppMaster jar from local filesystem and add to local environment");
		addToLocalResources(fileSystem, "huffmanyarnmultithreadv2.jar", "job.jar", appId.toString(), localResources, null);
		
		// Set local resource info into app master container launch context
		applicationMasterContainerContext.setLocalResources(localResources);

		// Set the environment variables to be setup in the env where the application master will be run
//		
		LOG.info("Set the environment for the application master");
		Map<String, String> environment = new HashMap<String, String>();

		// Environment variable CLASSPATH
		StringBuilder classPathEnvironment = new StringBuilder(Environment.CLASSPATH.$$()).append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
		for (String c : configuration.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
			classPathEnvironment.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			classPathEnvironment.append(c.trim());
		}
		environment.put("CLASSPATH", classPathEnvironment.toString());

		// Set environment
		applicationMasterContainerContext.setEnvironment(environment);

		// Set the necessary command to execute the application master
		Vector<CharSequence> applicationMasterArgs = new Vector<CharSequence>(30);

		// Set java executable command
		applicationMasterArgs.add(Environment.JAVA_HOME.$$() + "/bin/java");
		
		// Java virtual machine args
		applicationMasterArgs.add("-Xmx" + Defines.applicationMasterMemory + "m");
		
		// Class to execute
		applicationMasterArgs.add(ApplicationMaster.class.getName());
		
		// Job id
		applicationMasterArgs.add(appId.toString());
		
		// File to be compressed
		applicationMasterArgs.add(this.fileName);
		
		// Stdout file
		applicationMasterArgs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
		
		// Stderr file
		applicationMasterArgs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

		// Get final commmand to launch container
		StringBuilder command = new StringBuilder();
		for (CharSequence str : applicationMasterArgs) {
			command.append(str).append(" ");
		}
//
		LOG.info("Completed setting up app master command " + command.toString());
		List<String> commands = new ArrayList<String>();
		commands.add(command.toString());
		applicationMasterContainerContext.setCommands(commands);

		// Submit the application to the applications manager. SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest); Ignore the response as either a valid response object is returned on success or an exception thrown to denote some form of a failure
//		
		LOG.info("Submitting application to ASM");
		yarnClient.submitApplication(appicationMasterContext);

		// Monitor the application
		return monitorApplication(appId);
	}

	private boolean monitorApplication(ApplicationId appId) throws YarnException, IOException {
		while (true) {
			// Check app status
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
//				
				LOG.debug("Thread sleep in monitoring loop interrupted");
			}

			// Get application report for the appId we are interested in 
			ApplicationReport report = yarnClient.getApplicationReport(appId);

			YarnApplicationState state = report.getYarnApplicationState();
			FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
			if (YarnApplicationState.FINISHED == state) {
				if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
//					
					LOG.info("Application has completed successfully. Breaking monitoring loop");
					System.out.println("Application has completed successfully. Breaking monitoring loop");
					return true;
				} else {
//					
					LOG.info("Application did finished unsuccessfully." + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
					System.out.println("Application did finished unsuccessfully." + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
					return false;
				}
			} else if (YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
//				
				LOG.info("Application did not finish." + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
				System.out.println("Application did not finish." + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
				return false;
			}
		}
	}

// Melhorar essa parte 
	private void addToLocalResources(FileSystem fileSystem, String fileSrcPath, String fileDstPath, String appId, Map<String, LocalResource> localResources, String resources) throws IOException {
		String suffix = "HuffmanYarnMultithreadV2/" + appId + "/" + fileDstPath;
		Path dst = new Path(fileSystem.getHomeDirectory(), suffix);
		if (fileSrcPath == null) {
			FSDataOutputStream outputStream = null;
			try {
				outputStream = FileSystem.create(fileSystem, dst, new FsPermission((short) 0710));
				outputStream.writeUTF(resources);
			} finally {
				IOUtils.closeQuietly(outputStream);
			}
		} else {
			fileSystem.copyFromLocalFile(new Path(fileSrcPath), dst);
		}
		
		FileStatus scFileStatus = fileSystem.getFileStatus(dst);
		LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()), LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, scFileStatus.getLen(), scFileStatus.getModificationTime());
		localResources.put(fileDstPath, scRsrc);
	}
}
