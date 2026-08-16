// Copyright 2023 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package debuglogger

// The fiber-typed request and response loggers live in s3api/middlewares so
// that this package depends on nothing outside the standard library, which
// keeps the fiber/fasthttp stack out of the vendor tree of consumers that
// reach s3response but never reach s3api. This file is the small exported
// surface those loggers need.
//
// It is a separate file, and a wrapper layer rather than a rename, so that
// logger.go stays character-for-character as upstream versity/versitygw has
// it apart from the moved functions. Renaming the unexported identifiers
// there would conflict on every upstream merge, and an upstream commit adding
// a call to one of them would merge cleanly and then fail to compile.

const (
	Green  = green
	Yellow = yellow
	Blue   = blue

	Reset = reset

	// BoxWidth is the width every box in this package is drawn at, and the
	// only width the layout helpers agree on. Pass it to
	// PrintInsideHorizontalBorders rather than a width of your own.
	BoxWidth = boxWidth
)

// WrapInBox wraps the output of fn inside a styled box titled title.
//
// Does not check the debug flag; the caller must.
func WrapInBox(color Color, title string, fn func()) {
	wrapInBox(color, title, boxWidth, fn)
}

// PrintWrappedLine prints a key-value pair as one row of a box, wrapping the
// value if it exceeds the row width.
//
// Does not check the debug flag; the caller must.
func PrintWrappedLine(keyColor Color, key, value string) {
	printWrappedLine(keyColor, key, value)
}

// PrintBoxTitleLine prints a box title line, closed with corner characters
// when closing is true.
//
// Does not check the debug flag; the caller must.
func PrintBoxTitleLine(color Color, title string, closing bool) {
	printBoxTitleLine(color, title, boxWidth, closing)
}

// PrintHorizontalBorder prints a horizontal border, closed with corner
// characters when closing is true.
//
// Does not check the debug flag; the caller must.
func PrintHorizontalBorder(color Color, closing bool) {
	printHorizontalBorder(color, boxWidth, closing)
}
