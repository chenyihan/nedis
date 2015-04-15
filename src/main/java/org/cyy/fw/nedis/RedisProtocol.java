package org.cyy.fw.nedis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.ArrayList;
import java.util.List;

import org.cyy.fw.nedis.util.NedisException;
import org.cyy.fw.nedis.util.TextEncoder;

public final class RedisProtocol {
	private static final byte DOLLAR_BYTE = '$';
	private static final byte ASTERISK_BYTE = '*';
	private static final byte PLUS_BYTE = '+';
	private static final byte MINUS_BYTE = '-';
	private static final byte COLON_BYTE = ':';

	public static final String NULL = "null";
	private final static int[] SIZETABLE = { 9, 99, 999, 9999, 99999, 999999,
			9999999, 99999999, 999999999, Integer.MAX_VALUE };
	private final static byte[] DIGIT_TENS = { '0', '0', '0', '0', '0', '0',
			'0', '0', '0', '0', '1', '1', '1', '1', '1', '1', '1', '1', '1',
			'1', '2', '2', '2', '2', '2', '2', '2', '2', '2', '2', '3', '3',
			'3', '3', '3', '3', '3', '3', '3', '3', '4', '4', '4', '4', '4',
			'4', '4', '4', '4', '4', '5', '5', '5', '5', '5', '5', '5', '5',
			'5', '5', '6', '6', '6', '6', '6', '6', '6', '6', '6', '6', '7',
			'7', '7', '7', '7', '7', '7', '7', '7', '7', '8', '8', '8', '8',
			'8', '8', '8', '8', '8', '8', '9', '9', '9', '9', '9', '9', '9',
			'9', '9', '9', };

	private final static byte[] DIGIT_ONES = { '0', '1', '2', '3', '4', '5',
			'6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8',
			'9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1',
			'2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4',
			'5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7',
			'8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0',
			'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3',
			'4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6',
			'7', '8', '9', };

	private final static byte[] DIGITS = { '0', '1', '2', '3', '4', '5', '6',
			'7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
			'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
			'x', 'y', 'z' };

	public static ByteBuf generateRequest(BinaryCommand binaryCmd,
			ByteBufAllocator alloc) {
		final int INIT_SIZE = 1024;
		byte[] command = binaryCmd.getCommand();
		byte[][] args = binaryCmd.getArgs();
		ByteBuf byteBuf = alloc.buffer(INIT_SIZE);
		byteBuf.writeByte(RedisProtocol.ASTERISK_BYTE);
		writeIntCrLf(args == null ? 1 : args.length + 1, byteBuf);
		byteBuf.writeByte(RedisProtocol.DOLLAR_BYTE);
		writeIntCrLf(command.length, byteBuf);
		byteBuf.writeBytes(command);
		writeCrLf(byteBuf);

		if (args != null) {
			for (final byte[] arg : args) {
				byteBuf.writeByte(RedisProtocol.DOLLAR_BYTE);
				writeIntCrLf(arg.length, byteBuf);
				byteBuf.writeBytes(arg);
				writeCrLf(byteBuf);
			}
		}
		return byteBuf;
	}

	private static void writeIntCrLf(int value, ByteBuf byteBuf) {
		if (value < 0) {
			byteBuf.writeByte((byte) '-');
			value = -value;
		}

		int size = 0;
		while (value > SIZETABLE[size])
			size++;

		size++;
		// if (size >= buf.length - count) {
		// flushBuffer();
		// }

		int q, r;
		int charPos = size;
		byte[] buf = new byte[charPos];

		while (value >= 65536) {
			q = value / 100;
			r = value - ((q << 6) + (q << 5) + (q << 2));
			value = q;
			buf[--charPos] = DIGIT_ONES[r];
			buf[--charPos] = DIGIT_TENS[r];
		}

		for (;;) {
			q = (value * 52429) >>> (16 + 3);
			r = value - ((q << 3) + (q << 1));
			buf[--charPos] = DIGITS[r];
			value = q;
			if (value == 0)
				break;
		}
		byteBuf.writeBytes(buf);
		// count += size;

		writeCrLf(byteBuf);
	}

	private static void writeCrLf(ByteBuf byteBuf) {
		byteBuf.writeByte('\r');
		byteBuf.writeByte('\n');
	}

	public static boolean isNull(String text) {
		return text == null || NULL.equals(text);
	}

	public static Object parseResponse(ByteBuf byteBuf) {
		byte b = byteBuf.readByte();
		if (b == MINUS_BYTE) {
			parseError(byteBuf);
		} else if (b == ASTERISK_BYTE) {
			return parseMultiBulkReply(byteBuf);
		} else if (b == COLON_BYTE) {
			return parseInteger(byteBuf);
		} else if (b == DOLLAR_BYTE) {
			return parseBulkReply(byteBuf);
		} else if (b == PLUS_BYTE) {
			return parseStatusCodeReply(byteBuf);
		} else {
			throw new NedisException("Unknown reply: " + (char) b);
		}
		return null;
	}

	private static void parseError(ByteBuf byteBuf) {
		String message = readLine(byteBuf);
		throw new NedisException(message);
	}

	private static Long parseInteger(ByteBuf byteBuf) {
		String num = readLine(byteBuf);
		return Long.valueOf(num);
	}

	private static String parseStatusCodeReply(ByteBuf byteBuf) {
		return readLine(byteBuf);
	}

	private static byte[] parseBulkReply(ByteBuf byteBuf) {
		int len = Integer.parseInt(readLine(byteBuf));
		if (len == -1) {
			return TextEncoder.encode(NULL);
		}
		byte[] read = new byte[len];
		int offset = 0;
		byteBuf.readBytes(read, offset, len);
		// read 2 more bytes for the command delimiter
		byteBuf.readByte();
		byteBuf.readByte();

		// return TextEncoder.decode(read);
		return read;
	}

	private static List<Object> parseMultiBulkReply(ByteBuf byteBuf) {
		int num = Integer.parseInt(readLine(byteBuf));
		if (num == -1) {
			return new ArrayList<>();
		}
		List<Object> ret = new ArrayList<Object>(num);
		for (int i = 0; i < num; i++) {
			try {
				ret.add(parseResponse(byteBuf));
			} catch (NedisException e) {
				ret.add(e);
			}
		}
		return ret;
	}

	private static String readLine(ByteBuf byteBuf) {
		int b;
		byte c;
		StringBuilder sb = new StringBuilder();

		while (true) {
			b = byteBuf.readByte();
			if (b == '\r') {

				c = byteBuf.readByte();
				if (c == '\n') {
					break;
				}
				sb.append((char) b);
				sb.append((char) c);
			} else {
				sb.append((char) b);
			}
		}
		String reply = sb.toString();
		if (reply.length() == 0) {
			throw new NedisException(
					"It seems like server has closed the connection.");
		}
		return reply;
	}

}
