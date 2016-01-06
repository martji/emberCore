package com.sdp.hotspot;

import java.util.LinkedList;
import java.util.Queue;

public class RankItem {
	String key;
	Queue<CountItem> countList;
	final int maxSlice = 10;
	
	public RankItem(String key) {
		this.key = key;
		countList = new LinkedList<CountItem>();
	}
	
	public Double getWeight(int currentSlice) {
		Double result = 0.0;
		for (CountItem item : countList) {
			int sliceId = item.getSliceId();
			int count = item.getCount();
			result += count / Math.pow(10, currentSlice - sliceId);
		}
		return result;
	}
	
	public void addCount(CountItem item) {
		countList.offer(item);
		if (countList.size() > maxSlice) {
			countList.poll();
		}
	}
}
