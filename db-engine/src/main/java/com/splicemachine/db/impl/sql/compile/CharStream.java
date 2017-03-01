/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

/**
 * This interface describes a character stream that maintains line and
 * column number positions of the characters.  It also has the capability
 * to backup the stream to some extent.  An implementation of this
 * interface is used in the TokenManager implementation generated by
 * JavaCCParser.
 *
 * All the methods except backup can be implemented in any fashion. backup
 * needs to be implemented correctly for the correct operation of the lexer.
 * Rest of the methods are all used to get information like line number,
 * column number and the String that constitutes a token and are not used
 * by the lexer. Hence their implementation won't affect the generated lexer's
 * operation.
 */

public interface CharStream {

  /**
   * Returns the next character from the selected input.  The method
   * of selecting the input is the responsibility of the class
   * implementing this interface.  Can throw any java.io.IOException.
   */
  char readChar() throws java.io.IOException;

  /**
   * Returns the column position of the character last read.
   * @deprecated
   * @see #getEndColumn
   */
  int getColumn();

  /**
   * Returns the line number of the character last read.
   * @deprecated
   * @see #getEndLine
   */
  int getLine();

  /**
   * Returns the column number of the last character for current token (being
   * matched after the last call to BeginTOken).
   */
  int getEndColumn();

  /**
   * Returns the line number of the last character for current token (being
   * matched after the last call to BeginTOken).
   */
  int getEndLine();

  /**
   * Returns the column number of the first character for current token (being
   * matched after the last call to BeginTOken).
   */
  int getBeginColumn();

  /**
   * Returns the line number of the first character for current token (being
   * matched after the last call to BeginTOken).
   */
  int getBeginLine();

  /**
   * Backs up the input stream by amount steps. Lexer calls this method if it
   * had already read some characters, but could not use them to match a
   * (longer) token. So, they will be used again as the prefix of the next
   * token and it is the implemetation's responsibility to do this right.
   */
  void backup(int amount);

  /**
   * Returns the next character that marks the beginning of the next token.
   * All characters must remain in the buffer between two successive calls
   * to this method to implement backup correctly.
   */
  char BeginToken() throws java.io.IOException;

  /**
   * Returns a string made up of characters from the marked token beginning
   * to the current buffer position. Implementations have the choice of returning
   * anything that they want to. For example, for efficiency, one might decide
   * to just return null, which is a valid implementation.
   */
  String GetImage();

  /**
   * Returns an array of characters that make up the suffix of length 'len' for
   * the currently matched token. This is used to build up the matched string
   * for use in actions in the case of MORE. A simple and inefficient
   * implementation of this is as follows :
   *
   *   {
   *      String t = GetImage();
   *      return t.substring(t.length() - len, t.length()).toCharArray();
   *   }
   */
  char[] GetSuffix(int len);

  /**
   * The lexer calls this function to indicate that it is done with the stream
   * and hence implementations can free any resources held by this class.
   * Again, the body of this function can be just empty and it will not
   * affect the lexer's operation.
   */
  void Done();


  // This method was added to support ability to get the input
  // between two tokens.
  int getBeginOffset();

  // This method was added to support ability to get the input
  // between two tokens.
  int getEndOffset();

  // These methods were added to support re-initialization of CharStreams
  void ReInit(java.io.Reader dstream,
              int startline, int startcolumn, int buffersize);

  void ReInit(java.io.Reader dstream, int startline, int startcolumn);

  void ReInit(java.io.InputStream dstream, int startline,
              int startcolumn, int buffersize);

  void ReInit(java.io.InputStream dstream, int startline,
              int startcolumn);

}
