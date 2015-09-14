package com.github.marschall.spring.batch.performance;

import java.io.IOException;
import java.io.Writer;


@FunctionalInterface
public interface LineCallack<T> {
  
  void writeLine(Writer writer, T item) throws IOException;

}
