package com.ekaqu.lsmt.io;

import java.io.IOException;

public interface Appendable <K> {

  Appendable<K> append(K data) throws IOException;
}
