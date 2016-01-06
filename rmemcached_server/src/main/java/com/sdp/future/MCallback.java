package com.sdp.future;

import java.util.concurrent.CountDownLatch;
/**
 * @author martji
 */

/**
 * @param <V>
 */
public class MCallback<V> {
	V result = null;
	CountDownLatch latch;
	
	public MCallback (CountDownLatch latch) {
		this.latch = latch;
	}
	
	public V call() {
		return result;
	}
	
	public void gotdata(V out) {
		result = out;
		latch.countDown();
	}
}
