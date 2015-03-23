package br.ufrj.ppgi.huffmanyarnmultithreadv2;


public class Action {
	public enum ActionToTake {
		LOADINMEMORY,
		PROCESS
	}
	
	public ActionToTake action;
	public InputSplit inputSplit;
	
	public Action(ActionToTake actionToTake, InputSplit inputSplit) {
		this.action = actionToTake;
		this.inputSplit = inputSplit;
	}
}
