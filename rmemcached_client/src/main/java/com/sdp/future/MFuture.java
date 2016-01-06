package com.sdp.future;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.sdp.operation.BaseOperation;
/**
 * 
 * @author martji
 */
public class MFuture<V> implements Future<V> {
	
	CountDownLatch latch;
	BaseOperation<V> op;

	public MFuture(CountDownLatch latch, BaseOperation<V> op) {
		this.latch = latch;
		this.op = op;
	}

	public boolean cancel(boolean mayInterruptIfRunning) {
		// TODO Auto-generated method stub
		return false;
	}

	public V get() throws InterruptedException, ExecutionException {
		// TODO Auto-generated method stub
		return null;
	}

	public V get(long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		 if (!latch.await(timeout, unit)) {
			 return null;
		 } else {
			 try {
				return op.getMcallback().call();
			} catch (Exception e) {
				e.printStackTrace();
			}
		 }
		return null;
	}

	public boolean isCancelled() {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isDone() {
		// TODO Auto-generated method stub
		return false;
	}

}
