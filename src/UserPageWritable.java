import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserPageWritable implements WritableComparable<UserPageWritable> {

  private String userId;
  private String pageId;

  @Override
  public void readFields(DataInput in) throws IOException {
    userId = in.readUTF();
    pageId = in.readUTF();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(userId);
    out.writeUTF(pageId);
  }

  @Override
  public int compareTo(UserPageWritable o) {
//    return ComparisonChain.start().compare(userId, o.userId)
//        .compare(pageId, o.pageId).result();
	  return 0;
  }
}