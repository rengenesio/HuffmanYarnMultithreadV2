package br.ufrj.ppgi.huffmanyarnmultithreadv2;

public class BitUtility {
	
	public static void setBit(byte[] byteArray, int pos, boolean s) {
		int byteIndex = pos / 8;
		pos = 7 - (pos % 8);
		
		if (s)
			byteArray[byteIndex] |= 1 << pos;
		else
			byteArray[byteIndex] &= ~(1 << pos);
	}

	public static boolean checkBit(byte[] byteArray, int pos) {
		int byteIndex = pos / 8;
		pos = 7 - (pos % 8);
		
		int bit = byteArray[byteIndex] & (1 << 7 - pos);
		if (bit > 0)
			return true;

		return false;
	}

}
