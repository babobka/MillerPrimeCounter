package ru.babobka.miller.tester;

import ru.babobka.miller.util.MathUtil;

import java.math.BigInteger;

/**
 * Created by dolgopolov.a on 06.07.15.
 */
public class MillerRabinPrimeTester implements PrimeTester {

	@Override
	public boolean isPrime(long n) {

		if (n == 2L) {
			return true;
		} else if (n % 2L == 0 || n < 2L) {
			return false;
		} else {
			boolean goToA;
			int s = getPower(n);
			BigInteger t = BigInteger.valueOf((n - 1) / (long) (Math.pow(2, s)));
			BigInteger bigM = BigInteger.valueOf(n);
			long x;
			int innerS;
			int r = MathUtil.log(n);
			while (r != 0) {
				goToA = false;
				r--;
				long a = (long) (Math.random() * (n - 4L) + 2L);
				x = BigInteger.valueOf(a).modPow(t, bigM).longValue();
				if (x == 1L || x == n - 1) {
					continue;
				}
				innerS = s - 1;
				while (innerS != 0) {
					innerS--;
					x = (x * x) % n;
					if (x == 1L) {
						return false;
					} else if (x == n - 1L) {
						goToA = true;
						break;
					}
				}
				if (!goToA) {
					return false;
				}
			}

		}
		return true;
	}

	private static int getPower(long m) {
		long localM = m - 1;
		int i = 0;
		while (localM % 2L == 0) {
			localM = localM >> 1;
			i++;
		}
		return i;
	}


}
