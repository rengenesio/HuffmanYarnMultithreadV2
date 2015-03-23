package br.ufrj.ppgi.huffmanyarnmultithreadv2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import br.ufrj.ppgi.huffmanyarnmultithreadv2.decoder.yarn.DecoderClient;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.encoder.yarn.EncoderClient;


public class Main {

	public static void main(String[] args) throws Exception {
		if(args.length < 2) { System.out.println("Falta(m) parametro(s)!"); return; }
		
		// Indicates a encode task 
		boolean encoder = false;
		// Indicates a decode task
		boolean decoder = false;
		
		// File to be compressed
		String fileName = args[0];
		
		// Parse commandline args for encode/decode/both task
		switch(args[1]) {
		case "encoder":
			encoder = true;
			break;
			
		case "decoder":
			decoder = true;
			break;

		case "both":
			encoder = true;
			decoder = true;
			break;
		}

		// YARN configuration
		Configuration conf = new Configuration();

		// HDFS access
		FileSystem fs = FileSystem.get(conf);

		// Encode task
		if(encoder) {
			long totalTime, startTime, endTime;

			// Try delete the output directories
			try {
				fs.delete(new Path(fileName + Defines.pathSuffix + Defines.codificationFileName), true);
				fs.delete(new Path(fileName + Defines.pathSuffix + Defines.compressedSplitsPath), true);
			} catch(Exception ex) { }

			// Measures encode start time
			startTime = System.nanoTime();
			
			// Instantiates and run the YARN client
			EncoderClient client = new EncoderClient(fileName);
			if (client.run()) { 
				System.out.println("Compressão completa!");
			}
			else {
				System.out.println("Erro durante a compressão");
				
				// Disables the decode task
				decoder = false;
			}
			// Measures encode end time
			endTime = System.nanoTime();
			
			// Calculates time span
			totalTime = endTime - startTime;
			
			System.out.println(totalTime/1000000000.0 + " s (encoder)");
		}

		// Decode task
		if(decoder) {
			long totalTime, startTime, endTime;
			
			// Try delete the output directories
			try {
				fs.delete(new Path(fileName + Defines.pathSuffix + Defines.decompressedSplitsPath), true);
			} catch(Exception ex) { }
			
			// Measures decode start time
			startTime = System.nanoTime();
			
			// Instantiates and run the YARN client
			DecoderClient client = new DecoderClient(fileName);
			if (client.run()) { 
				System.out.println("Descompressão completa!");
			}
			else {
				System.out.println("Erro durante a descompressão");
			}
			// Measures decode end time 
			endTime = System.nanoTime();
			
			// Calculates time span
			totalTime = endTime - startTime;
			
			System.out.println(totalTime / 1000000000.0 + " s (decoder)");
		}
	}
}
