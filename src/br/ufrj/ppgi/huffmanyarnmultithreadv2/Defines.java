package br.ufrj.ppgi.huffmanyarnmultithreadv2;


public class Defines {
	public static final String jobName = "HuffmanYarnMultithreadV2"; 
	
	// YARN Application Master defines
	public static final int amMemory = 8;
	public static final int amVCores = 1;
	public static final int amPriority = 0;
	public static final String amQueue = "default";
	
	// YARN container defines
	public static final int containerMemory = 4096;
	public static final int containerVCores = 8;
	
	// Compression performance defines
	public static final int maxThreads = containerVCores * 2;
	public static final int readBufferSize = 8192;
	public static final int maxChunksInMemory = 32;
	
	// Path defines
	public static final String pathSuffix = ".yarnmultithreadv2dir/";
	public static final String compressedPath = "compressed/";
	public static final String compressedFileName = "part-";
	public static final String codificationFileName = "codification";
	public static final String decompressedFileName = "decompressed";
	
	
	// Huffman algorithm constants
	public static final int bitsCodification = 8;
	public static final int twoPowerBitsCodification = 256;
}
