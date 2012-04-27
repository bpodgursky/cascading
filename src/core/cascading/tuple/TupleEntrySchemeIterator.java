/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.tuple;

import java.io.Closeable;
import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.scheme.ConcreteCall;
import cascading.scheme.Scheme;
import cascading.util.CloseableIterator;
import cascading.util.SingleCloseableInputIterator;

/**
 *
 */
public class TupleEntrySchemeIterator<Process extends FlowProcess<Config>, Config, SourceContext, Input> extends TupleEntryIterator
  {
  private final Process flowProcess;
  private final Scheme<Process, Config, Input, ?, SourceContext, ?> scheme;
  private final CloseableIterator<Input> inputIterator;
  private ConcreteCall<SourceContext, Input> sourceCall;

  private String identifier;
  private boolean isComplete = false;
  private boolean hasWaiting = false;
  private TupleException currentException;

  public TupleEntrySchemeIterator( Process flowProcess, Scheme<Process, Config, Input, ?, SourceContext, ?> scheme, Input input )
    {
    this( flowProcess, scheme, input, null );
    }

  public TupleEntrySchemeIterator( Process flowProcess, Scheme<Process, Config, Input, ?, SourceContext, ?> scheme, Input input, String identifier )
    {
    this( flowProcess, scheme, (CloseableIterator<Input>) new SingleCloseableInputIterator( (Closeable) input ), identifier );
    }

  public TupleEntrySchemeIterator( Process flowProcess, Scheme<Process, Config, Input, ?, SourceContext, ?> scheme, CloseableIterator<Input> inputIterator )
    {
    this( flowProcess, scheme, inputIterator, null );
    }

  public TupleEntrySchemeIterator( Process flowProcess, Scheme<Process, Config, Input, ?, SourceContext, ?> scheme, CloseableIterator<Input> inputIterator, String identifier )
    {
    super( scheme.getSourceFields() );
    this.flowProcess = flowProcess;
    this.scheme = scheme;
    this.inputIterator = inputIterator;
    this.identifier = identifier;

    if( this.identifier == null || this.identifier.isEmpty() )
      this.identifier = "'unknown'";

    if( !inputIterator.hasNext() )
      {
      isComplete = true;
      return;
      }

    sourceCall = new ConcreteCall<SourceContext, Input>();

    sourceCall.setIncomingEntry( getTupleEntry() );
    sourceCall.setInput( wrapInput( inputIterator.next() ) );

    try
      {
      this.scheme.sourcePrepare( flowProcess, sourceCall );
      }
    catch( IOException exception )
      {
      throw new TupleException( "unable to prepare source for input identifier: " + this.identifier, exception );
      }
    }

  protected Process getFlowProcess()
    {
    return flowProcess;
    }

  protected Input wrapInput( Input input )
    {
    return input;
    }

  @Override
  public boolean hasNext()
    {
    if( isComplete )
      return false;

    if( hasWaiting )
      return true;

    try
      {
      getNext();
      }
    catch( Exception exception )
      {
      if( identifier == null || identifier.isEmpty() )
        identifier = "'unknown'";

      currentException = new TupleException( "unable to read from input identifier: " + identifier, exception );
      return true;
      }

    if( !hasWaiting )
      isComplete = true;

    return !isComplete;
    }

  private TupleEntry getNext() throws IOException
    {
    Tuples.asModifiable( sourceCall.getIncomingEntry().getTuple() );
    hasWaiting = scheme.source( flowProcess, sourceCall );

    if( !hasWaiting && inputIterator.hasNext() )
      {
      sourceCall.setInput( wrapInput( inputIterator.next() ) );

      return getNext();
      }

    return getTupleEntry();
    }

  @Override
  public TupleEntry next()
    {
    try
      {
      if( currentException != null )
        throw currentException;
      }
    finally
      {
      currentException = null; // data may be trapped
      }

    if( isComplete )
      throw new IllegalStateException( "no next element" );

    try
      {
      if( hasWaiting )
        return getTupleEntry();

      return getNext();
      }
    catch( Exception exception )
      {
      throw new TupleException( "unable to source from input identifier: " + identifier, exception );
      }
    finally
      {
      hasWaiting = false;
      }
    }

  @Override
  public void remove()
    {
    throw new UnsupportedOperationException( "may not remove elements from this iterator" );
    }

  @Override
  public void close() throws IOException
    {
    try
      {
      if( sourceCall != null )
        scheme.sourceCleanup( flowProcess, sourceCall );
      }
    finally
      {
      inputIterator.close();
      }
    }
  }