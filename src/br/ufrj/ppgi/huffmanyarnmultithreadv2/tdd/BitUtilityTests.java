package br.ufrj.ppgi.huffmanyarnmultithreadv2.tdd;

import br.ufrj.ppgi.huffmanyarnmultithreadv2.BitUtility;

public class BitUtilityTests {

	public static boolean checkBitTest() throws Exception {
		System.out.println("Testando utilitário de bits");
		
		
		byte[] byteArray = new byte[5];
		byteArray[0] = 10; // 00001010
		byteArray[1] = 38; // 00100110
		byteArray[2] = 15; // 00000111
		byteArray[3] = -1; // 11111111
		byteArray[4] = 98; // 01100010
		
		if (BitUtility.checkBit(byteArray, 0) != false) throw new Exception("Erro checando bit! " + 0);
		if (BitUtility.checkBit(byteArray, 1) != false) throw new Exception("Erro checando bit! " + 1);
		if (BitUtility.checkBit(byteArray, 2) != false) throw new Exception("Erro checando bit! " + 2);
		if (BitUtility.checkBit(byteArray, 3) != false) throw new Exception("Erro checando bit! " + 3);
		if (BitUtility.checkBit(byteArray, 4) != true) throw new Exception("Erro checando bit! " + 4);
		if (BitUtility.checkBit(byteArray, 5) != false) throw new Exception("Erro checando bit! " + 5);
		if (BitUtility.checkBit(byteArray, 6) != true) throw new Exception("Erro checando bit! " + 6);
		if (BitUtility.checkBit(byteArray, 7) != false) throw new Exception("Erro checando bit! " + 7);
		if (BitUtility.checkBit(byteArray, 8) != false) throw new Exception("Erro checando bit! " + 8);
		if (BitUtility.checkBit(byteArray, 9) != false) throw new Exception("Erro checando bit! " + 9);
		if (BitUtility.checkBit(byteArray, 10) != true) throw new Exception("Erro checando bit! " + 10);
		if (BitUtility.checkBit(byteArray, 11) != false) throw new Exception("Erro checando bit! " + 11);
		if (BitUtility.checkBit(byteArray, 12) != false) throw new Exception("Erro checando bit! " + 12);
		if (BitUtility.checkBit(byteArray, 13) != true) throw new Exception("Erro checando bit! " + 13);
		if (BitUtility.checkBit(byteArray, 14) != true) throw new Exception("Erro checando bit! " + 14);
		if (BitUtility.checkBit(byteArray, 15) != false) throw new Exception("Erro checando bit! " + 15);
		if (BitUtility.checkBit(byteArray, 16) != false) throw new Exception("Erro checando bit! " + 16);
		if (BitUtility.checkBit(byteArray, 17) != false) throw new Exception("Erro checando bit! " + 17);
		if (BitUtility.checkBit(byteArray, 18) != false) throw new Exception("Erro checando bit! " + 18);
		if (BitUtility.checkBit(byteArray, 19) != false) throw new Exception("Erro checando bit! " + 19);
		if (BitUtility.checkBit(byteArray, 20) != false) throw new Exception("Erro checando bit! " + 20);
		if (BitUtility.checkBit(byteArray, 21) != true) throw new Exception("Erro checando bit! " + 21);
		if (BitUtility.checkBit(byteArray, 22) != true) throw new Exception("Erro checando bit! " + 22);
		if (BitUtility.checkBit(byteArray, 23) != true) throw new Exception("Erro checando bit! " + 23);
		if (BitUtility.checkBit(byteArray, 24) != true) throw new Exception("Erro checando bit! " + 24);
		if (BitUtility.checkBit(byteArray, 25) != true) throw new Exception("Erro checando bit! " + 25);
		if (BitUtility.checkBit(byteArray, 26) != true) throw new Exception("Erro checando bit! " + 26);
		if (BitUtility.checkBit(byteArray, 27) != true) throw new Exception("Erro checando bit! " + 27);
		if (BitUtility.checkBit(byteArray, 28) != true) throw new Exception("Erro checando bit! " + 28);
		if (BitUtility.checkBit(byteArray, 29) != true) throw new Exception("Erro checando bit! " + 29);
		if (BitUtility.checkBit(byteArray, 30) != true) throw new Exception("Erro checando bit! " + 30);
		if (BitUtility.checkBit(byteArray, 31) != true) throw new Exception("Erro checando bit! " + 31);
		if (BitUtility.checkBit(byteArray, 32) != false) throw new Exception("Erro checando bit! " + 32);
		if (BitUtility.checkBit(byteArray, 33) != true) throw new Exception("Erro checando bit! " + 33);
		if (BitUtility.checkBit(byteArray, 34) != true) throw new Exception("Erro checando bit! " + 34);
		if (BitUtility.checkBit(byteArray, 35) != false) throw new Exception("Erro checando bit! " + 35);
		if (BitUtility.checkBit(byteArray, 36) != false) throw new Exception("Erro checando bit! " + 36);
		if (BitUtility.checkBit(byteArray, 37) != false) throw new Exception("Erro checando bit! " + 37);
		if (BitUtility.checkBit(byteArray, 38) != true) throw new Exception("Erro checando bit! " + 38);
		if (BitUtility.checkBit(byteArray, 39) != false) throw new Exception("Erro checando bit! " + 39);
	
	
		return true;
	}

}
