package com.github.marschall.spring.batch.performance;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;

/**
 * Streams data to a file and provides transaction support.
 * 
 * <p>Unlike
 * {@link org.springframework.batch.item.file.FlatFileItemWriter} this
 * class does not buffer the entire content of the file and is
 * therefore suited for large files.</p>
 * 
 * <p>The implementation is <b>not</b> thread-safe.<p>
 * 
 * <p>Resuming is not supported.<p>
 */
@NotThreadSafe
public class StreamingFlatFileWriter<T> implements ItemStreamWriter<T> {
  
  // configuration variables
  private Consumer<Writer> headerCallback;
  private LineCallack<T> lineCallack = (writer, item) -> writer.write(item.toString());
  private Consumer<Writer> footerCallback;
  private String lineSeparator = System.getProperty("line.separator");
  private String encoding = System.getProperty("file.encoding");
  private Path resource;
  private Path workResource;
  private Function<Path, Path> workResourceCreator = path -> path.resolveSibling(path.getFileName() + ".work");
  private int bufferSize = 8192;
  
  // work variables
  private Writer writer;
  private long linesWritten;
  
  private void openWriter() throws IOException {
    if (writer != null) {
      throw new ItemStreamException("ItemStream already open.");
    }
    this.workResource = this.workResourceCreator.apply(this.resource);
    OutputStream outputStream = Files.newOutputStream(this.workResource);
    Writer unbufferedWriter = new OutputStreamWriter(outputStream, encoding);
    this.writer = new BufferedWriter(unbufferedWriter, bufferSize);
  }
  
  private void commit() throws IOException {
    this.writer.close(); // also flushes
    this.writer = null;
    try {
      Files.move(workResource, resource, StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException e) {
      Files.move(workResource, resource);
    }
    // TODO fsync parent directory on Linux
  }
  
  public void setLineAggregator(Function<T, String> lineAggregator) {
    this.setLineCallack((writer, item) -> writer.write(lineAggregator.apply(item)));
  }
  
  public void setLineCallack(LineCallack<T> lineCallack) {
    this.lineCallack = lineCallack;
  }

  @Override
  public void write(List<? extends T> items) throws Exception {
    for (T item : items) {
      this.lineCallack.writeLine(this.writer, item);
      this.writer.write(this.lineSeparator);
    }
    linesWritten += items.size();
  }

  @Override
  public void open(ExecutionContext executionContext) throws ItemStreamException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void update(ExecutionContext executionContext) throws ItemStreamException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void close() throws ItemStreamException {
    // TODO Auto-generated method stub
    
  }
  
  

}
