package br.ufrj.ppgi.huffmanyarnmultithreadv2.encoder.yarn;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.Records;

import br.ufrj.ppgi.huffmanyarnmultithreadv2.Defines;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.InputSplit;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.encoder.Encoder;


public class ApplicationMaster {

	private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
	
	// Configuration
	private Configuration configuration;

	// String with the Application Id
	private String appId;
	
	// FileName to be compressed
	private String fileName;
	
	// Hostname where the master container will be executed
	private String masterContainerHostName;

	// Handle to communicate with the Resource Manager
	@SuppressWarnings("rawtypes")
	private AMRMClientAsync resourceManagerClient;

	// Handle to communicate with the Node Manager
	private NMClientAsync nodeManagerClient;
	
	// Listen to process the response from the Node Manager
	private NMCallbackHandler containerListener;

	// Application Attempt Id ( combination of attemptId and fail count )
	protected ApplicationAttemptId appAttemptID;
	
	// Tracking url to which app master publishes info for clients to monitor
	private String appMasterTrackingUrl = "";

	// Number of containers to be launched
	private int numTotalContainers = 1;
	
	// Counter for completed containers (complete denotes successful or failed)
	private AtomicInteger numCompletedContainers = new AtomicInteger();

	// Allocated container count so that we know how many containers has the RM allocated to us
	protected AtomicInteger numAllocatedContainers = new AtomicInteger();

	// Count of failed containers
	private AtomicInteger numFailedContainers = new AtomicInteger();
	
	// Count of containers already requested from the RM needed as once requested, we should not request for containers again.	Only request for more if the original requirement changes.
	protected AtomicInteger numRequestedContainers = new AtomicInteger();

	// Indicates if job is done
	private volatile boolean done;

	// Launch threads
	private List<Thread> launchedThreads = new ArrayList<Thread>();
	
	private ByteBuffer allTokens;

	// Maps a host to a file input split (offset and length)
	private HashMap<String, ArrayList<String>> hostInputSplit = new HashMap<String, ArrayList<String>>();
	

	public ApplicationMaster(String[] args) {
		this.configuration = new YarnConfiguration();
		this.appId = args[0];
		this.fileName = args[1];
	}

	@SuppressWarnings("unchecked")
	public void run() throws YarnException, IOException, InterruptedException {
		LOG.info("Starting ApplicationMaster");
		
		Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
		DataOutputBuffer dob = new DataOutputBuffer();
		credentials.writeTokenStorageToStream(dob);
		
		// Now remove the AM->RM token so that containers cannot access it.
		Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
		while (iter.hasNext()) {
			Token<?> token = iter.next();
			LOG.info(token);
			if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
				iter.remove();
			}
		}
		allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
	    
		
		AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
		resourceManagerClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
		resourceManagerClient.init(configuration);
		resourceManagerClient.start();

		containerListener = new NMCallbackHandler(this);
		nodeManagerClient = new NMClientAsyncImpl(containerListener);
		nodeManagerClient.init(configuration);
		nodeManagerClient.start();
		
		// Get hostname where ApplicationMaster is running
		String appMasterHostname = NetUtils.getHostname();
		
		// Register self with ResourceManager. This will start heartbeating to the RM
		resourceManagerClient.registerApplicationMaster(appMasterHostname, -1, appMasterTrackingUrl);
		
		// Priority for worker containers - priorities are intra-application
	    Priority priority = Records.newRecord(Priority.class);
	    priority.setPriority(0);

	    // Resource requirements for worker containers
	    Resource capability = Records.newRecord(Resource.class);
	    capability.setMemory(Defines.containerMemory);
	    capability.setVirtualCores(Defines.containerVCores);

	    // Resolver for hostname/rack
		RackResolver.init(configuration);
		
		// Search blocks from file
		Path path = new Path(fileName);
		FileSystem fileSystem = FileSystem.get(path.toUri(), configuration);
		
		FileStatus fileStatus = fileSystem.getFileStatus(path);
		BlockLocation[] blockLocationArray = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		LOG.info("# of blocks: " + blockLocationArray.length);
		
		int i = 0;
		for(BlockLocation blockLocation : blockLocationArray) {
			String hostName = blockLocation.getHosts()[0];
			if(blockLocation.getOffset() == 0) {
				this.masterContainerHostName = hostName;
			}
			if(hostInputSplit.containsKey(hostName) == false) {
				hostInputSplit.put(hostName, new ArrayList<String>());
			}
			InputSplit inputSplit = new InputSplit(this.fileName, i, blockLocation.getOffset(), (int) blockLocation.getLength());
			hostInputSplit.get(hostName).add(inputSplit.toString());
			
			for(String s : blockLocation.getHosts()) {
				LOG.debug("HostLocation " + i + ": " + s + ", offset: " + blockLocation.getOffset() + ", length: " + blockLocation.getLength());
				System.out.println("HostLocation " + i + ": " + s + ", offset: " + blockLocation.getOffset() + ", length: " + blockLocation.getLength());
			}
			
			i++;
		}
		
		
		Iterator<Map.Entry<String, ArrayList<String>>> it = hostInputSplit.entrySet().iterator();
		while(it.hasNext()) {
			Map.Entry<String, ArrayList<String>> hashEntry = it.next();
			
			String[] containerLocation = { hashEntry.getKey() };
			ContainerRequest containerAsk = new ContainerRequest(capability, containerLocation, null, priority, false);
			resourceManagerClient.addContainerRequest(containerAsk);
		}
		
		numTotalContainers = hostInputSplit.size();
		LOG.info("numTotalContainers: " + numTotalContainers);

		numRequestedContainers.set(numTotalContainers);
	}
	
	//Thread to connect to the {@link ContainerManagementProtocol} and launch the container that will execute the shell command.
	private class LaunchContainerRunnable implements Runnable {

		// Allocated container
		Container container;

		NMCallbackHandler containerListener;

		/**
		 * @param lcontainer
		 *            Allocated container
		 * @param containerListener
		 *            Callback handler of the container
		 */
		public LaunchContainerRunnable(Container lcontainer, NMCallbackHandler containerListener) {
			this.container = lcontainer;
			this.containerListener = containerListener;
		}

		@Override
		/**
		 * Connects to CM, sets up container launch context 
		 * for shell command and eventually dispatches the container 
		 * start request to the CM. 
		 */
		public void run() {
			LOG.info("Setting up container launch container for containerid=" + container.getId() + ", " + container.getNodeId() + ", " + container.getNodeHttpAddress());
			
			ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

			Map<String, String> env = new HashMap<String, String>();

			// Env variable CLASSPATH
			StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$()).append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
			for (String c : configuration.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
				classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
				classPathEnv.append(c.trim());
			}
			env.put("CLASSPATH", classPathEnv.toString());

			// Set the environment
			ctx.setEnvironment(env);
			
			// Instance of FileSystem
			FileSystem fs = null;
			try {
				fs = FileSystem.get(configuration);
			} catch (IOException e) {
				e.printStackTrace();
			}

			// Set local resources for the application master local files or archives as needed. In this scenario, the jar file for the application master is part of the local resources
			Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
			
			// TODO: consertar este workaround
			Random random = new Random();
			int randomInt = random.nextInt();
			
			// Copy the application master jar to the filesystem. Create a local resource to point to the destination jar path
			LOG.info("Copying AppMaster jar from local filesystem and add to local environment: job" + randomInt + ".jar");
			try {
				
				addToLocalResources(fs, "job.jar", "job" + randomInt + ".jar", appId.toString(), localResources, null);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			// Set local resource info into app master container launch context
			ctx.setLocalResources(localResources);
			
			// Set the necessary command to execute the container
			Vector<CharSequence> vargs = new Vector<CharSequence>(30);

			// Set java executable command
			vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");

			// Java virtual machine args
			vargs.add("-Xmx" + Defines.containerMemory + "m");
			
			// Class to execute
			vargs.add(Encoder.class.getName());
			
			// File to be compressed
			vargs.add(fileName);
			
			// String collection with the splits for this node container
			ArrayList<String> splitsForNode = hostInputSplit.get(container.getNodeId().getHost());

			// Container Id (indicates the input split part)
			vargs.add(StringUtils.join(splitsForNode, ":"));
			
			// Master container hostname
			vargs.add(masterContainerHostName);
			
			// Number of launched containers
			vargs.add(Integer.toString(numTotalContainers));
			
			// Stdout file
			vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
			
			// Stderr file
			vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

			// Get final commmand
			StringBuilder command = new StringBuilder();
			for (CharSequence str : vargs) {
				command.append(str).append(" ");
			}

			LOG.info("Completed setting up container command: " + command.toString());
			List<String> commands = new ArrayList<String>();
			commands.add(command.toString());
			ctx.setCommands(commands);
			
			ctx.setTokens(allTokens.duplicate());
			
			containerListener.addContainer(container.getId(), container);
			nodeManagerClient.startContainerAsync(container, ctx);
		}
		
		private void addToLocalResources(FileSystem fs, String fileSrcPath, String fileDstPath, String appId, Map<String, LocalResource> localResources, String resources) throws IOException {
			String suffix = "/user/admin/HuffmanYarnMultithreadV2/" + appId + "/" + fileDstPath;
			Path dst = new Path(fs.getHomeDirectory(), suffix);
			if (fileSrcPath == null) {
				FSDataOutputStream ostream = null;
				try {
					ostream = FileSystem.create(fs, dst, new FsPermission((short) 0710));
					ostream.writeUTF(resources);
				} finally {
					IOUtils.closeQuietly(ostream);
				}
			} else {
				fs.copyFromLocalFile(new Path(fileSrcPath), dst);
			}
			
			FileStatus scFileStatus = fs.getFileStatus(dst);
			LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()), LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, scFileStatus.getLen(), scFileStatus.getModificationTime());
			localResources.put(fileDstPath, scRsrc);
		}
	}
		
	// Wait for job completion
	protected boolean finish() {

		while (!done && (numCompletedContainers.get() != numTotalContainers)) {
			try {
				Thread.sleep(200);
			} catch (InterruptedException ex) { }
		}

		// Join all launched threads needed for when we time out and we need to release containers
		for (Thread launchThread : launchedThreads) {
			try {
				launchThread.join(10000);
			} catch (InterruptedException e) {
				LOG.info("Exception thrown in thread join: " + e.getMessage());
				e.printStackTrace();
			}
		}

		// When the application completes, it should stop all running containers
//		
		LOG.info("Application completed. Stopping running containers");
		nodeManagerClient.stop();

		// When the application completes, it should send a finish application signal to the RM
//		
		LOG.info("Application completed. Signalling finish to RM");

		FinalApplicationStatus appStatus;
		String appMessage = null;
		boolean success = true;
		if (numFailedContainers.get() == 0 && numCompletedContainers.get() == numTotalContainers) {
			appStatus = FinalApplicationStatus.SUCCEEDED;
		} else {
			appStatus = FinalApplicationStatus.FAILED;
			appMessage = "Diagnostics." + ", total=" + numTotalContainers + ", completed=" + numCompletedContainers.get() + ", allocated=" + numAllocatedContainers.get() + ", failed=" + numFailedContainers.get();
			success = false;
		}
		
		try {
			resourceManagerClient.unregisterApplicationMaster(appStatus, appMessage, null);
		} catch (Exception ex) {
			LOG.error("Failed to unregister application", ex);
		}
		
		resourceManagerClient.stop();

		return success;
	}

	private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
		@Override
		public void onContainersCompleted(List<ContainerStatus> completedContainers) {
			LOG.info("Got response from RM for container ask, completedCnt=" + completedContainers.size());
			for (ContainerStatus containerStatus : completedContainers) {
				LOG.info("Got container status for containerID=" + containerStatus.getContainerId() + ", state=" + containerStatus.getState() + ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics=" + containerStatus.getDiagnostics());

				// non complete containers should not be here
				assert (containerStatus.getState() == ContainerState.COMPLETE);

				// increment counters for completed/failed containers
				int exitStatus = containerStatus.getExitStatus();
				if (0 != exitStatus) {
					// container failed
					if (ContainerExitStatus.ABORTED != exitStatus) {
						// shell script failed counts as completed
						numCompletedContainers.incrementAndGet();
						numFailedContainers.incrementAndGet();
					} else {
						// container was killed by framework, possibly preempted we should re-try as the container was lost for some reason
						numAllocatedContainers.decrementAndGet();
						numRequestedContainers.decrementAndGet();
						// we do not need to release the container as it would be done by the RM
					}
				} else {
					// nothing to do container completed successfully
					numCompletedContainers.incrementAndGet();
					LOG.info("Container completed successfully." + ", containerId=" + containerStatus.getContainerId());
				}
			}

			// ask for more containers if any failed
			int askCount = numTotalContainers - numRequestedContainers.get();
			numRequestedContainers.addAndGet(askCount);

			if (askCount > 0) {
				try {
					throw new Exception("Algum container falhou");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			if (numCompletedContainers.get() == numTotalContainers) {
				done = true;
			}
		}

		@Override
		public void onContainersAllocated(List<Container> allocatedContainers) {
//			
			LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
			numAllocatedContainers.addAndGet(allocatedContainers.size());
			for (Container allocatedContainer : allocatedContainers) {
//				
				LOG.info("Launching shell command on a new container." + ", containerId=" + allocatedContainer.getId() + ", containerNode=" + allocatedContainer.getNodeId().getHost() + ":" + allocatedContainer.getNodeId().getPort() + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress() + ", containerResourceMemory" + allocatedContainer.getResource().getMemory()+ ", containerResourceVirtualCores" + allocatedContainer.getResource().getVirtualCores());
				System.out.println("ContainerId= " + allocatedContainer.getId() + ", containerNode= " + allocatedContainer.getNodeId().getHost() + ":" + allocatedContainer.getNodeId().getPort() + ", containerResourceMemory" + allocatedContainer.getResource().getMemory()+ ", containerResourceVirtualCores" + allocatedContainer.getResource().getVirtualCores());
				LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, containerListener);
				Thread launchThread = new Thread(runnableLaunchContainer);

				// launch and start the container on a separate thread to keep the main thread unblocked as all containers may not be allocated at one go.
				launchedThreads.add(launchThread);
				launchThread.start();
			}
		}

		@Override
		public void onShutdownRequest() {
			done = true;
		}

		@Override
		public void onNodesUpdated(List<NodeReport> updatedNodes) {
		}

		@Override
		public float getProgress() {
			// set progress to deliver to RM on next heartbeat
			float progress = (float) numCompletedContainers.get() / numTotalContainers;
			return progress;
		}

		@Override
		public void onError(Throwable e) {
			done = true;
			resourceManagerClient.stop();
		}
	}

	static class NMCallbackHandler implements NMClientAsync.CallbackHandler {

		private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();
		private final ApplicationMaster applicationMaster;

		public NMCallbackHandler(ApplicationMaster applicationMaster) {
			this.applicationMaster = applicationMaster;
		}

		public void addContainer(ContainerId containerId, Container container) {
			containers.putIfAbsent(containerId, container);
		}

		@Override
		public void onContainerStopped(ContainerId containerId) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Succeeded to stop Container " + containerId);
			}
			containers.remove(containerId);
		}

		@Override
		public void onContainerStatusReceived(ContainerId containerId,
				ContainerStatus containerStatus) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Container Status: id=" + containerId + ", status="
						+ containerStatus);
			}
		}

		@Override
		public void onContainerStarted(ContainerId containerId,
				Map<String, ByteBuffer> allServiceResponse) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Succeeded to start Container " + containerId);
			}
			Container container = containers.get(containerId);
			if (container != null) {
				applicationMaster.nodeManagerClient.getContainerStatusAsync(
						containerId, container.getNodeId());
			}
		}

		@Override
		public void onStartContainerError(ContainerId containerId, Throwable t) {
			LOG.error("Failed to start Container " + containerId);
			containers.remove(containerId);
			applicationMaster.numCompletedContainers.incrementAndGet();
			applicationMaster.numFailedContainers.incrementAndGet();
		}

		@Override
		public void onGetContainerStatusError(ContainerId containerId,
				Throwable t) {
			LOG.error("Failed to query the status of Container " + containerId);
		}

		@Override
		public void onStopContainerError(ContainerId containerId, Throwable t) {
			LOG.error("Failed to stop Container " + containerId);
			containers.remove(containerId);
		}
	}
	
	
	
	public static void main(String[] args) throws YarnException, IOException, InterruptedException {
		ApplicationMaster applicationMaster = new ApplicationMaster(args);
		applicationMaster.run();
		applicationMaster.finish();
	}
}
