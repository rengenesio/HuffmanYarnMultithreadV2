package br.ufrj.ppgi.huffmanyarnmultithreadv2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import br.ufrj.ppgi.huffmanyarnmultithreadv2.decoder.yarn.DecoderClient;
import br.ufrj.ppgi.huffmanyarnmultithreadv2.encoder.yarn.EncoderClient;


public class Main {

	public static void main(String[] args) throws Exception {
		if(args.length < 2) { System.out.println("Falta(m) parametro(s)!"); return; }
		
		boolean encoder = false;
		boolean decoder = false;
		
		String fileName = args[0];
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
		
		if(encoder) {
			long totalTime, startTime, endTime;

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
	
			try {
				fs.delete(new Path(fileName + Defines.pathSuffix), true);
			} catch(Exception ex) { }
				
			startTime = System.nanoTime();
			EncoderClient client = new EncoderClient(args);
			if (client.run()) { 
				System.out.println("Compressão completa!");
			}
			else {
				System.out.println("Erro durante a compressão");
				endTime = System.nanoTime();
				
				totalTime = endTime - startTime;
				
				System.out.println(totalTime/1000000000.0 + " s (encoder)");
				return;
			}
			endTime = System.nanoTime();
			
			totalTime = endTime - startTime;
			
			System.out.println(totalTime/1000000000.0 + " s (encoder)");
		}

		if(decoder) {
			long totalTime, startTime, endTime;
			
			startTime = System.nanoTime();
			DecoderClient client = new DecoderClient(args);
			if (client.run()) { 
				System.out.println("Descompressão completa!");
			}
			else {
				System.out.println("Erro durante a descompressão");
				endTime = System.nanoTime();
				
				totalTime = endTime - startTime;
				
				System.out.println(totalTime/1000000000.0 + " s (encoder)");
				return;
			}
			endTime = System.nanoTime();
			
			totalTime = endTime - startTime;
			
			System.out.println(totalTime / 1000000000.0 + " s (decoder)");
		}
	}
}
