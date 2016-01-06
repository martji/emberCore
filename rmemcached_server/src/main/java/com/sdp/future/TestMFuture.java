package com.sdp.future;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.sdp.operation.BaseOperation;
/**
 * 
 * @author martji
 *
 */
public class TestMFuture {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		final CountDownLatch latch = new CountDownLatch(1);
		final BaseOperation<Integer> op = new BaseOperation<Integer>(new MCallback<Integer>(latch));
        
		MFuture<Integer> future = new MFuture<Integer>(latch, op);
		
		new Thread(new Runnable() {
			public void run() {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				op.getMcallback().gotdata(1);
			}
		}).start();
		
		try {
			System.out.println(future.get(3000, TimeUnit.MILLISECONDS));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
