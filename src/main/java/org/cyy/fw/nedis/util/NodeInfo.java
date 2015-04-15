package org.cyy.fw.nedis.util;

public class NodeInfo {
	private int weight;
	private String name;

	public int getWeight() {
		if (weight == 0) {
			weight = 1;
		}
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
