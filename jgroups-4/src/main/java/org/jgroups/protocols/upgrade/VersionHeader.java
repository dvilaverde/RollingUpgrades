package org.jgroups.protocols.upgrade;

import org.jgroups.Header;
import org.jgroups.util.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

public class VersionHeader extends Header {

  public short version;

  public VersionHeader() {
  }

  public VersionHeader(short version) {
    this.version = version;
  }

  @Override
  public short getMagicId() {
    return 2342;
  }

  @Override
  public Supplier<? extends Header> create() {
    return VersionHeader::new;
  }

  @Override
  public int serializedSize() {
    return 2;
  }

  @Override
  public void writeTo(DataOutput dataOutput) throws IOException {
    dataOutput.writeShort(version);
  }

  @Override
  public void readFrom(DataInput dataInput) throws IOException {
    this.version = dataInput.readShort();
  }
}
