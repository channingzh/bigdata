package com.test;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

public class Test {

	private static String dir = "C:\\Users\\channing\\Documents\\Tencent Files\\1106899075\\FileRecv\\";
	private static String filename = "test.txt.zip";
	private static String txtfilename = "test.txt";

	public static void main(String[] args) throws Exception {
		// System.out.println( 18647173 -2097152 - 16777216);
		unzip(new File(dir+filename),dir,"");
		readFile();
	}

	public static void readFile() throws Exception {
		final int BUFFER_SIZE = 0x300000;// 缓冲区大小为3M

		long perFileSize = 3 * 1024 * 1024;// 1.5G
		File f = new File(dir + txtfilename);
		long flength = f.length();
		long fileCount = flength / perFileSize;

		byte[] dst = new byte[BUFFER_SIZE];// 每次读出3M的内容

		long start = System.currentTimeMillis();
		HashMap<String, Integer> strCountMap = new HashMap<String, Integer>();
		Set<WordEntity> set = new TreeSet<WordEntity>();
		long startPostion = 0;
		for (int j = 0; j <= fileCount; ++j) {
			// 超出文件长度
			if (startPostion + perFileSize >= flength) {
				perFileSize = f.length() - startPostion;
			}
			// 分割 map(FileChannel.MapMode mode,long position, long size)
			MappedByteBuffer inputBuffer = new RandomAccessFile(f, "r").getChannel().map(FileChannel.MapMode.READ_ONLY, startPostion, perFileSize);
			// 每个块建立一个输出
			// FileWriter output = new FileWriter(dir + "part" + j + ".txt");

			for (int offset = 0; offset < inputBuffer.capacity(); offset += BUFFER_SIZE) {
				if (inputBuffer.capacity() - offset >= BUFFER_SIZE) {
					for (int i = 0; i < BUFFER_SIZE; i++)
						dst[i] = inputBuffer.get(offset + i);
				} else {
					for (int i = 0; i < inputBuffer.capacity() - offset; i++)
						dst[i] = inputBuffer.get(offset + i);
				}

				int length = (inputBuffer.capacity() % BUFFER_SIZE == 0) ? BUFFER_SIZE : inputBuffer.capacity() % BUFFER_SIZE;
				// output.append(new String(dst, 0, length,"UTF-8"));
				String strtmp = new String(dst, 0, length, "UTF-8");
				StringTokenizer st = new StringTokenizer(strtmp, ";");
				set = counts(st, strCountMap, set);

			}

			/*
			 * output.flush(); output.close(); output = null;
			 */
			startPostion += perFileSize;
		}
		long end = System.currentTimeMillis();
		print(set);
		System.out.println("分割文件耗时：" + (end - start) + "毫秒");
	}

	// 计数
	public static Set<WordEntity> counts(StringTokenizer st, HashMap<String, Integer> map, Set<WordEntity> set) throws IOException {
		while (st.hasMoreTokens()) {
			String letter = st.nextToken();
			int count;
			if (map.get(letter) == null) {
				count = 1;
			} else {
				count = map.get(letter).intValue() + 1;
			}
			map.put(letter, count);
		}
		Set<WordEntity> result = set;
		for (String key : map.keySet()) {
			result.add(new WordEntity(key, map.get(key)));
		}
		return result;
	}

	// 打印前5
	public static void print(Set<WordEntity> set) throws IOException {
		int count = 1;
		for (Iterator<WordEntity> it = set.iterator(); it.hasNext();) {
			WordEntity w = it.next();
			System.out.println("第" + count + "名为单词:" + w.getKey() + ",出现的次数为： " + w.getCount());
			if (count == 5)// 当输出5个后跳出循环
				break;
			count++;
		}
	}

	public static void unzip(File zipFile, String dest, String passwd) throws ZipException {
		ZipFile zFile = new ZipFile(zipFile); // 首先创建ZipFile指向磁盘上的.zip文件
		zFile.setFileNameCharset("UTF-8"); // 设置文件名编码，在UTF-8系统中需要设置
		if (!zFile.isValidZipFile()) { // 验证.zip文件是否合法，包括文件是否存在、是否为zip文件、是否被损坏等
			throw new ZipException("压缩文件不合法,可能被损坏.");
		}
		File destDir = new File(dest); // 解压目录
		if (destDir.isDirectory() && !destDir.exists()) {
			destDir.mkdir();
		}
		if (zFile.isEncrypted()) {
			zFile.setPassword(passwd.toCharArray()); // 设置密码
		}
		zFile.extractAll(dest); // 将文件抽出到解压目录(解压)
	}

	public static void splitFile() throws IOException {
		long timer = System.currentTimeMillis();
		int bufferSize = 20 * 1024 * 1024;// 设读取文件的缓存为20MB

		// 建立缓冲文本输入流
		File file = new File("big.txt.zip");
		FileInputStream fileInputStream = new FileInputStream(file);
		BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
		InputStreamReader inputStreamReader = new InputStreamReader(bufferedInputStream);
		BufferedReader input = new BufferedReader(inputStreamReader, bufferSize);

		int splitNum = 112 - 1;// 要分割的块数减一
		int fileLines = 23669283;// 输入文件的行数
		long perSplitLines = fileLines / splitNum;// 每个块的行数
		for (int i = 0; i <= splitNum; ++i) {
			// 分割
			// 每个块建立一个输出
			FileWriter output = new FileWriter("part" + i + ".txt");

			String line = null;
			// 逐行读取，逐行输出
			for (long lineCounter = 0; lineCounter < perSplitLines && (line = input.readLine()) != null; ++lineCounter) {
				output.append(line);
			}
			output.flush();
			output.close();
			output = null;
		}
		input.close();
		timer = System.currentTimeMillis() - timer;
		System.out.println("处理时间：" + timer);

	}

	/*
	 * public static void readZipFile(String file) throws Exception { ZipFile zf
	 * = new ZipFile(file); InputStream in = new BufferedInputStream(new
	 * FileInputStream(file)); ZipInputStream zin = new ZipInputStream(in);
	 * ZipEntry ze; while ((ze = zin.getNextEntry()) != null) { if
	 * (ze.isDirectory()) { } else {
	 * 
	 * long size = ze.getSize(); if (size > 0) { BufferedReader br = new
	 * BufferedReader(new InputStreamReader(zf.getInputStream(ze))); String
	 * line; while ((line = br.readLine()) != null) { System.out.println(line);
	 * } br.close(); } System.out.println(); } } zin.closeEntry(); }
	 */

	public static void splitRead() throws Exception {
		final int BUFFER_SIZE = 0x300000;// 缓冲区大小为3M

		File f = new File(filename);

		MappedByteBuffer inputBuffer = new RandomAccessFile(f, "r").getChannel().map(FileChannel.MapMode.READ_ONLY, f.length() / 2, f.length() / 2);

		byte[] dst = new byte[BUFFER_SIZE];// 每次读出3M的内容

		long start = System.currentTimeMillis();

		for (int offset = 0; offset < inputBuffer.capacity(); offset += BUFFER_SIZE) {

			if (inputBuffer.capacity() - offset >= BUFFER_SIZE) {

				for (int i = 0; i < BUFFER_SIZE; i++)

					dst[i] = inputBuffer.get(offset + i);

			} else {

				for (int i = 0; i < inputBuffer.capacity() - offset; i++)

					dst[i] = inputBuffer.get(offset + i);

			}

			int length = (inputBuffer.capacity() % BUFFER_SIZE == 0) ? BUFFER_SIZE : inputBuffer.capacity() % BUFFER_SIZE;

			System.out.println(new String(dst, 0, length));// new
			// String(dst,0,length)这样可以取出缓存保存的字符串，可以对其进行操作

		}

		long end = System.currentTimeMillis();

		System.out.println("读取文件文件一半内容花费：" + (end - start) + "毫秒");
	}
}
