package br.ufrj.ppgi.huffmanyarnmultithreadv2.decoder;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Queue;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import br.ufrj.ppgi.huffmanyarnmultithreadv2.Defines;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.InputSplit;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.SerializationUtility;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.encoder.Action;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.encoder.BitSet;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.encoder.Codification;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.encoder.Encoder;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.encoder.HostPortPair;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.encoder.NodeArray;


public class Decoder {
	// ------------- MASTER AND SLAVE CONTAINER PROPERTIES ------------- //
	
	// (YARN) YARN Configuration
	private Configuration configuration;
	
	// (YARN) Indicates if this container is the master container
	private boolean containerIsMaster = false;
	
	// (YARN) Total executing containers 
	private int numTotalContainers;
		
	// File to be processed
	private String fileName;
	
	// Collection with this container's input splits to be processed
	private ArrayList<InputSplit> inputSplitCollection;
	
	// Total input splits for container
	private int numTotalInputSplits = 0;
	
	// Array of byte arrays (each byte array represents a input split byte sequence) 
	private byte[][] memory;
	
	// Container total frequency array
	private long[] containerTotalFrequencyArray;
	
	// Matrix to store each thread's frequency array
	private long[][] frequencyMatrix;
	
	// Número de símbolos que o container encontrou
	private short symbols = 0;
	
	// Array with Huffman codes
	private Codification[] codificationArray;
	
	// Total threads to be spawn
	private int numTotalThreads = 1;
	
	// Total chunks to be loaded in memory
	private int numTotalChunksInMemory = 0;
	
	// Queue to store sequencial id's (starts at 0) to threads (symbol count threads and encoder threads)
	private Queue<Integer> symbolCountOrderedThreadIdQueue;
	
	// Queue to store input splits indicator (symbol count threads and encoder queues)
	private Queue<Action> globalThreadActionQueue;
	
	int loadedChunks = 0;
	boolean memoryFull = false;
	
	//private Queue<InputSplit> symbolCountInputSplitMetadataQueue;
	//private Queue<InputSplit> encoderInputSplitMetadataQueue;
	

	// ------------------ MASTER CONTAINER PROPERTIES ------------------ //
	
	// (YARN) Master stores slaves containers listening ports
	private HostPortPair[] containerPortPairArray;
	
	// Total containers frequency array sum
	private long[] totalFrequencyArray;
	
	// Huffman's node array
	private NodeArray nodeArray;
	
	
	// ------------------ SLAVE CONTAINER PROPERTIES ------------------- //
	
	// (YARN) Master container hostname
	private String masterContainerHostName;
	
	// Port where slave container will listen for master connection
	private int slavePort;
	
	
	
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
			InputSplit inputSplit = new InputSplit(inputSplitFieldsCollection[0], Integer.parseInt(inputSplitFieldsCollection[0]), Long.parseLong(inputSplitFieldsCollection[1]), Integer.parseInt(inputSplitFieldsCollection[2]));
			
			// Add this input split to input split collection
			this.inputSplitCollection.add(inputSplit);

			// The master container will be the one with the part 0
			if(inputSplit.part == 0) {
				this.containerIsMaster = true;
			}
			
//
			System.out.println(inputSplit);
		}
		
		// Sets number of total input splits for this container
		this.numTotalInputSplits = this.inputSplitCollection.size();
		
		// Reads the master container hostname from command line args
		this.masterContainerHostName = args[2];
		
		// Reads the number of total containers from command line args
		this.numTotalContainers = Integer.parseInt(args[3]);
		
		// Initializes the queues with  
		//this.symbolCountInputSplitMetadataQueue = new ArrayBlockingQueue<InputSplit>(this.numTotalInputSplits, true);
		//this.encoderInputSplitMetadataQueue = new ArrayBlockingQueue<InputSplit>(this.numTotalInputSplits, true);
	}
	
//	public void fileToCodification() throws IOException {
//		FileSystem fs = FileSystem.get(new Configuration());
//		FSDataInputStream f = fs.open(cb);
//
//		byte[] byteArray = new byte[f.available()];
//		f.readFully(byteArray);
//
//		this.codificationArray = SerializationUtility.deserializeCodificationArray(byteArray);
//		
//		/*
//		System.out.println("CODIFICATION: symbol (size) code"); 
//		for(short i = 0 ; i < symbols ; i++)
//			System.out.println(codificationArray[i].toString());
//		*/
//	}
//
//	public void codeToTreeArray() {
//		for(short i = 0 ; i < this.codificationArray.length ; i++) {
//			this.max_code = (this.codificationArray[i].size > this.max_code) ? this.codificationArray[i].size : this.max_code;  
//		}
//		
//		codificationArrayElementSymbol = new byte[(int) Math.pow(2, (max_code + 1))];
//		codificationArrayElementUsed = new boolean[(int) Math.pow(2, (max_code + 1))];
//
//		for (short i = 0; i < this.codificationArray.length; i++) {
//			int index = 0;
//			for (byte b : codificationArray[i].code) {
//				index <<= 1;
//				if (b == 0)
//					index += 1;
//				else
//					index += 2;
//			}
//			codificationArrayElementSymbol[index] = codificationArray[i].symbol;
//			codificationArrayElementUsed[index] = true;
//		}
//
//		/*
//		System.out.println("codeToTreeArray():");
//		System.out.println("TREE_ARRAY:"); 
//		for(int i = 0 ; i < Math.pow(2,(max_code + 1)) ; i++) 
//			if(codificationArrayElementUsed[i])
//				System.out.println("i: " + i + " -> " + codificationArrayElementSymbol[i]);
//		System.out.println("------------------------------");
//		*/
//	}
//	
//	public void huffmanDecode() throws IOException {
//		byte[] buffer = new byte[1];
//		BitSet bufferBits = new BitSet();
//		
//		int index = 0;
//
//		FileSystem fs = FileSystem.get(new Configuration());
//		FileStatus[] status = fs.listStatus(in);
//		FSDataOutputStream fout = fs.create(out);
//
//		for (short i = 0; i < status.length; i++) {
//			FSDataInputStream fin = fs.open(status[i].getPath());
//
//			while (fin.available() > 0) {
//				fin.read(buffer, 0, 1);
//				bufferBits.fromByte(buffer[0]);
//				for (byte j = 0; j < 8 ; j++) {
//					index <<= 1;
//					if (bufferBits.cheackBit(j) == false)
//						index += 1;
//					else
//						index += 2;
//
//					if (codificationArrayElementUsed[index]) {
//						if (codificationArrayElementSymbol[index] != 0) {
//							fout.write(codificationArrayElementSymbol[index]);
//							index = 0;
//						} else {
//							index = 0;
//							break;
//						}
//					}
//				}
//			}
//			fin.close();
//		}
//		fout.close();
//	}
	
	
	
	
	public static void main(String[] args) throws IOException, InterruptedException {
		Decoder decoder = new Decoder(args);
//		decoder.decode();
	}
}