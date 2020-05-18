/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package lifecycle

import (
	"encoding/xml"
)

var (
	errInvalidFilter = Errorf("Filter must have exactly one of Prefix, Tag, or And specified")
)

// Filter - a filter for a lifecycle configuration Rule.
type Filter struct {
	XMLName xml.Name `xml:"Filter"`
	Prefix  string
	And     And
	Tag     Tag
}

// MarshalXML - produces the xml representation of the Filter struct
// only one of Prefix, And and Tag should be present in the output.
func (f Filter) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if err := e.EncodeToken(start); err != nil {
		return err
	}

	switch {
	case !f.And.isEmpty():
		if err := e.EncodeElement(f.And, xml.StartElement{Name: xml.Name{Local: "And"}}); err != nil {
			return err
		}
	case !f.Tag.IsEmpty():
		if err := e.EncodeElement(f.Tag, xml.StartElement{Name: xml.Name{Local: "Tag"}}); err != nil {
			return err
		}
	default:
		// Always print Prefix field when both And & Tag are empty
		if err := e.EncodeElement(f.Prefix, xml.StartElement{Name: xml.Name{Local: "Prefix"}}); err != nil {
			return err
		}
	}

	return e.EncodeToken(xml.EndElement{Name: start.Name})
}

// Validate - validates the filter element
func (f Filter) Validate() error {
	// A Filter must have exactly one of Prefix, Tag, or And specified.
	if !f.And.isEmpty() {
		if f.Prefix != "" {
			return errInvalidFilter
		}
		if !f.Tag.IsEmpty() {
			return errInvalidFilter
		}
		if err := f.And.Validate(); err != nil {
			return err
		}
	}
	if f.Prefix != "" {
		if !f.Tag.IsEmpty() {
			return errInvalidFilter
		}
	}
	if !f.Tag.IsEmpty() {
		if err := f.Tag.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// isEmpty - returns true if Filter tag is empty
func (f Filter) isEmpty() bool {
	return f.And.isEmpty() && f.Prefix == "" && f.Tag.IsEmpty()
}
