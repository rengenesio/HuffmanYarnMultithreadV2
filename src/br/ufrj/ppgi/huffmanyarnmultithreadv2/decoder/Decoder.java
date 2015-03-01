package br.ufrj.ppgi.huffmanyarnmultithreadv2.decoder;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import br.ufrj.ppgi.huffmanyarnmultithreadv2.BitUtility;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.Defines;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.InputSplit;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.SerializationUtility;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.encoder.Action;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.encoder.Action.ActionToTake;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.encoder.Codification;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.encoder.HostPortPair;


public class Decoder {
	// ------------- MASTER AND SLAVE CONTAINER PROPERTIES ------------- //
	
	// (YARN) YARN Configuration
	private Configuration configuration;
	
	// (YARN) Indicates if this container is the master container
	//private boolean containerIsMaster = false;
	
	// (YARN) Total executing containers 
	//private int numTotalContainers;
		
	// File to be processed
	private String fileName;
	
	// Collection with this container's input splits to be processed
	private ArrayList<InputSplit> inputSplitCollection;
	
	// Total input splits for container
	private int numTotalInputSplits = 0;
	
	// Array with Huffman codes
	private Codification[] codificationArray;
	
	// Total threads to be spawn
	private int numTotalThreads = 1;
	
	// Queue to store input splits indicator (symbol count threads and encoder queues)
	private Queue<Action> globalThreadActionQueue;
	
	int loadedChunks = 0;
	boolean memoryFull = false;
	
	byte max_code = 0;
	byte[] codificationArrayElementSymbol;
	boolean[] codificationArrayElementUsed;


	// ------------------ MASTER CONTAINER PROPERTIES ------------------ //

	// (YARN) Master stores slaves containers listening ports
	//private HostPortPair[] containerPortPairArray;
	

	// ------------------ SLAVE CONTAINER PROPERTIES ------------------- //
	
	// (YARN) Master container hostname
	//private String masterContainerHostName;
	
	// Port where slave container will listen for master connection
	//private int slavePort;
	
	
	
	public Decoder(String[] args) {
		// Instantiates a YARN configuration
		this.configuration = new Configuration();

		// Reads filename from command line args
		this.fileName = args[0];

		// Instantiates a collection to store input splits metadata
		this.inputSplitCollection = new ArrayList<InputSplit>();
		
		// Splits command line arg in strings, each one represents an input split
		String[] inputSplitStringCollection = StringUtils.split(args[1], ':');
		
		// Iterates each string that represents an input split
		for(String inputSplitString : inputSplitStringCollection) {
			// Split an input split string in 3 fields (part, offset and length)
			String[] inputSplitFieldsCollection = StringUtils.split(inputSplitString, '-');
			
			// Instantiates a new input split
			InputSplit inputSplit = new InputSplit(inputSplitFieldsCollection[0], Integer.parseInt(inputSplitFieldsCollection[1]), Long.parseLong(inputSplitFieldsCollection[2]), Integer.parseInt(inputSplitFieldsCollection[3]));
			
			// Add this input split to input split collection
			this.inputSplitCollection.add(inputSplit);

			// The master container will be the one with the part 0
			//if(inputSplit.part == 0) {
			//	this.containerIsMaster = true;
			//}
			
//
			System.out.println(inputSplit);
		}
		
		// Sets number of total input splits for this container
		this.numTotalInputSplits = this.inputSplitCollection.size();
		
		//// Reads the master container hostname from command line args
		//this.masterContainerHostName = args[2];
		
		//// Reads the number of total containers from command line args
		//this.numTotalContainers = Integer.parseInt(args[3]);
	}
	
	public void decode() throws IOException, InterruptedException {
		fileToCodification();
		codeToTreeArray();
		
		// Ideal thread number (a thread per input split)
		int idealNumThreads = this.numTotalInputSplits;

		// Limitates the thread number to the max for this container or to the ideal number of threads
		this.numTotalThreads = (idealNumThreads > Defines.maxThreads ? Defines.maxThreads : idealNumThreads);
		
		// Enqueue initial actions (load some chunks in memory and process input splits that will not be loaded in memory)
		this.globalThreadActionQueue = new ArrayBlockingQueue<Action>(this.numTotalInputSplits);
		for(int i = 0 ; i < this.numTotalInputSplits ; i++) {
			this.globalThreadActionQueue.add(new Action(ActionToTake.PROCESS, inputSplitCollection.get(i)));
		}
		
		// Collection to store the spawned threads
		ArrayList<Thread> threadCollection = new ArrayList<Thread>();
		for(int i = 0 ; i < numTotalThreads ; i++) {
			Thread thread = new Thread(new Runnable() {
				
				// File access variable
				FileSystem fileSystem = FileSystem.get(configuration);
				
				@Override
				public void run() {
					// Thread loop until input split metadata queue is empty
					while(globalThreadActionQueue.isEmpty() == false) {
						// Takes an action from the action queue
						Action action = globalThreadActionQueue.poll();

						try {
							huffmanDecompressor(action.inputSplit);
						} catch (Exception e) {
							e.printStackTrace();
							System.err.println("Exception comprimindo o arquivo!");
						}
					}
				}
				
				public void huffmanDecompressor(InputSplit inputSplit) throws IOException {
					Path pathIn = new Path(fileName + Defines.pathSuffix + Defines.compressedSplitsPath + inputSplit.fileName);
					FSDataInputStream inputStream = fileSystem.open(pathIn);
					
					System.out.println("PathIn: " + pathIn.toString());
										
					Path pathOut = new Path(fileName + Defines.pathSuffix + Defines.decompressedSplitsPath + inputSplit.fileName);
					FSDataOutputStream outputStream = fileSystem.create(pathOut);
					
					// Buffer to store data to be written in disk
					byte[] bufferOutput = new byte[Defines.writeBufferSize];
					int bufferOutputIndex = 0;
					
					// Buffer to store read from disk
					byte[] bufferInput = new byte[Defines.readBufferSize];

					int readBytes = 0;
					int totalReadBytes = 0;
					int codificationArrayIndex = 0;
					do {
						readBytes = inputStream.read(inputSplit.offset + totalReadBytes, bufferInput, 0, (totalReadBytes + Defines.readBufferSize > inputSplit.length ? inputSplit.length - totalReadBytes : Defines.readBufferSize));

						for (int i = 0; i < readBytes * 8 ; i++) {
							codificationArrayIndex <<= 1;
							if (BitUtility.checkBit(bufferInput, i) == false)
								codificationArrayIndex += 1;
							else
								codificationArrayIndex += 2;

							if (codificationArrayElementUsed[codificationArrayIndex]) {
								if (codificationArrayElementSymbol[codificationArrayIndex] != 0) {
									byte symbol = codificationArrayElementSymbol[codificationArrayIndex];
									bufferOutput[bufferOutputIndex] = symbol;
									
									bufferOutputIndex++;
									if(bufferOutputIndex >= Defines.writeBufferSize) {
										outputStream.write(bufferOutput, 0, bufferOutputIndex);
										bufferOutputIndex = 0;
									}
									codificationArrayIndex = 0;
								} else {
									if(bufferOutputIndex > 0) {
										outputStream.write(bufferOutput, 0, bufferOutputIndex);
									}
									
									outputStream.close();
									inputStream.close();
									return;
								}
							}
						}
					} while (readBytes > 0);
				}
			});
			
			// Add thread to the collection
			threadCollection.add(thread);
			
			// Starts thread
			thread.start();
		}
		
		// Wait until all threads finish their jobs
		for(Thread thread : threadCollection) {
			thread.join();
		}
	}
	
	public void fileToCodification() throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataInputStream f = fs.open(new Path(this.fileName + Defines.pathSuffix + Defines.codificationFileName));

		byte[] byteArray = new byte[f.available()];
		f.readFully(byteArray);

		this.codificationArray = SerializationUtility.deserializeCodificationArray(byteArray);
		
		/*
		System.out.println("CODIFICATION: symbol (size) code"); 
		for(short i = 0 ; i < symbols ; i++)
			System.out.println(codificationArray[i].toString());
		*/
	}

	public void codeToTreeArray() {
		for(short i = 0 ; i < this.codificationArray.length ; i++) {
			this.max_code = (this.codificationArray[i].size > this.max_code) ? this.codificationArray[i].size : this.max_code;  
		}
		
		codificationArrayElementSymbol = new byte[(int) Math.pow(2, (max_code + 1))];
		codificationArrayElementUsed = new boolean[(int) Math.pow(2, (max_code + 1))];

		for (short i = 0; i < this.codificationArray.length; i++) {
			int index = 0;
			for (byte b : codificationArray[i].code) {
				index <<= 1;
				if (b == 0)
					index += 1;
				else
					index += 2;
			}
			codificationArrayElementSymbol[index] = codificationArray[i].symbol;
			codificationArrayElementUsed[index] = true;
		}

		/*
		System.out.println("codeToTreeArray():");
		System.out.println("TREE_ARRAY:"); 
		for(int i = 0 ; i < Math.pow(2,(max_code + 1)) ; i++) 
			if(codificationArrayElementUsed[i])
				System.out.println("i: " + i + " -> " + codificationArrayElementSymbol[i]);
		System.out.println("------------------------------");
		*/
	}

	
	
	public static void main(String[] args) throws IOException, InterruptedException {
		Decoder decoder = new Decoder(args);
		decoder.decode();
	}
}