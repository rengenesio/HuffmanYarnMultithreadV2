package br.ufrj.ppgi.huffmanyarnmultithreadv2;


public class Defines {
	public static final String jobName = "HuffmanYarnMultithreadV2"; 
	
	// YARN Application Master defines
	public static final int applicationMasterMemory = 8;
	public static final int applicationMasterVCores = 1;
	public static final int applicationNasterPriority = 0;
	public static final String applicationMasterQueue = "default";
	
	// YARN container defines
	public static final int containerMemory = 4096;
	public static final int containerVCores = 8;
	
	// Compression performance defines
	public static final int maxThreads = containerVCores;
	public static final int readBufferSize = 16384;
	public static final int writeBufferSize = 16384;
	public static final int maxChunksInMemory = 16;
	
	// Path defines
	public static final String pathSuffix = ".yarnmultithreadv2dir/";
	public static final String compressedSplitsPath = "compressed/";
	public static final String compressedSplitFileName = "part_";
	public static final String codificationFileName = "codification";
	public static final String decompressedSplitsPath = "decompressed-part/";
	public static final String decompressedSplitFileName = "decompressed-part_";
	public static final String decompressedFileName = "decompressed";
	
	
	
	
	public static final int bitsInByte= 8;
	
	// Huffman algorithm constants
	public static final int bitsCodification = 8;
	public static final int twoPowerBitsCodification = 256;
	public static final int huffmanTreeMaxPath = 32; 
}
