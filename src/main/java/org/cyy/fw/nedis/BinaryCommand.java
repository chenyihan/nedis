package org.cyy.fw.nedis;

import org.cyy.fw.nedis.util.TextEncoder;

public class BinaryCommand {
	private byte[] command;
	private byte[][] args;

	public BinaryCommand(String command, String... args) {
		super();
		this.command = TextEncoder.encode(command);
		if (args != null && args.length > 0) {
			this.args = new byte[args.length][];
			for (int i = 0; i < args.length; i++) {
				this.args[i] = TextEncoder.encode(args[i]);
			}
		}
	}

	public BinaryCommand(String command, byte[]... args) {
		super();
		this.command = TextEncoder.encode(command);
		this.args = args;
	}

	public BinaryCommand(byte[] command, byte[]... args) {
		super();
		this.command = command;
		this.args = args;
	}

	public byte[] getCommand() {
		return command;
	}

	public void setCommand(byte[] command) {
		this.command = command;
	}

	public byte[][] getArgs() {
		return args;
	}

	public void setArgs(byte[][] args) {
		this.args = args;
	}
}
