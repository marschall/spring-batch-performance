package com.github.marschall.spring.batch.performance;

import java.io.Writer;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Streams data to a file and provides transaction support.
 * 
 * <p>Unlike
 * {@link org.springframework.batch.item.file.FlatFileItemWriter} this
 * class does not buffer the entire content of the file and is
 * therefore suited for large files.</p>
 */
public class StreamingFlatFileWriter<T> {
  
  private Consumer<Writer> headerCallback;
  private LineCallack<T> lineCallack = (writer, item) -> writer.write(item.toString());
  private Consumer<Writer> footerCallback;
  private String lineSeparator = System.getProperty("line.separator");
  private Path resource;
  private Path workResource;
  private Function<Path, Path> workResourceCreator = path -> path.resolveSibling(path.getFileName() + ".work");
  private int bufferSize = 8192;
  
  public void setLineAggregator(Function<T, String> lineAggregator) {
    this.setLineCallack((writer, item) -> writer.write(lineAggregator.apply(item)));
  }
  
  public void setLineCallack(LineCallack<T> lineCallack) {
    this.lineCallack = lineCallack;
  }
  
  

}
