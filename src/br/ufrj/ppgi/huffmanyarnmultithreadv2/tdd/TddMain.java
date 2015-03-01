package br.ufrj.ppgi.huffmanyarnmultithreadv2.tdd;


public class TddMain {

	public static void main(String[] args) throws Exception {
		if(SerializationUtilityTests.serializeAndDeserializeFrequencyArrayTest()) {
			System.out.println("Testes de serializar e deserializar Frequency Array ok!!");
		}
		
		if(BitUtilityTests.checkBitTest()) {
			System.out.println("Testes de checar os bits ok!!");
		}
	
	}
}
