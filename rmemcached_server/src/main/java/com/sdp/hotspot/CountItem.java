package com.sdp.hotspot;

public class CountItem {
	int sliceId;
	int count;
	
	public CountItem(int sliceId, int count) {
		this.sliceId = sliceId;
		this.count = count;
	}
	
	public int getSliceId() {
		return sliceId;
	}
	public void setSliceId(int sliceId) {
		this.sliceId = sliceId;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
}
