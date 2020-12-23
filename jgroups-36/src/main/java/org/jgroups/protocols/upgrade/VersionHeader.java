package org.jgroups.protocols.upgrade;

import org.jgroups.Header;

import java.io.DataInput;
import java.io.DataOutput;

public class VersionHeader extends Header {

  public short version;

  public VersionHeader() {
  }

  public VersionHeader(short version) {
    this.version = version;
  }

  @Override
  public int size() {
    return 2;
  }

  @Override
  public void writeTo(DataOutput out) throws Exception {
    out.writeShort(version);
  }

  @Override
  public void readFrom(DataInput in) throws Exception {
    version = in.readShort();
  }
}
