package br.ufrj.ppgi.huffmanyarnmultithreadv2;


public class InputSplit implements Comparable<InputSplit> {

	public int part;
	public long offset;
	public int length;
	public String fileName; 

	public InputSplit() {
	}

	public InputSplit(String fileName, int part, long offset, int length) {
		this.fileName = fileName;
		this.part = part;
		this.offset = offset;
		this.length = length;
	}

	@Override
	public String toString() {
		return new String(this.fileName + "-" + this.part + "-" + this.offset + "-" + this.length); 
	}

	@Override
	public int compareTo(InputSplit anotherInputSplit) {
		if(this.fileName.compareTo(anotherInputSplit.fileName) < 0) {
			return -1;
		} else if (this.fileName.compareTo(anotherInputSplit.fileName) == 0) {
			if(this.part < anotherInputSplit.part) {
				return -1;
			} else if (this.part == anotherInputSplit.part) {
				return 0;	
			} else if (this.part > anotherInputSplit.part) {
				return 1;	
			}
		} else {
			return 1;
		}
		
		return 0;
	}
}
