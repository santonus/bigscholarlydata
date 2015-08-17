package org.dblp.model;
import java.util.HashMap;
import java.util.Map;

import org.textanalysis.ir.SpecialTextStringDocument;

import ir.utilities.Weight;
import ir.vsr.Document;
import ir.vsr.HashMapVector;
public class BibEntry {
	private String _entryId;
	private String _title;
	private String _abstract;
	private HashMapVector _titleBoW= null;
	private HashMapVector _abstractBoW= null;
	public String getEntryId() {
		return _entryId;
	}
	public void setEntryId(String entryId) {
		_entryId = entryId;
	}
	public String getTitle() {
		return _title;
	}
	public String getTitleAsBOW() {
		return getBOWAsStr(_titleBoW);
	}
	public void setTitle(String title) {
		_title = title;
		_titleBoW = (new SpecialTextStringDocument(this._title)).hashMapVector();
	}
	public String getAbstract() {
		return _abstract;
	}
	public String getAbstractBOW() {
		return getBOWAsStr(_abstractBoW);
	}
	
	public void setAbstract(String abstract1) {
		_abstract = abstract1;
		_abstractBoW = (new SpecialTextStringDocument(this._abstract)).hashMapVector();
	}
	
	private String getBOWAsStr(HashMapVector vector) {
		StringBuffer strbuf = new StringBuffer();
		if (vector==null)
			return strbuf.toString();
		for (Map.Entry<String,Weight> entry : vector.entrySet()) {
			int count = (int)entry.getValue().getValue();
			String word = entry.getKey();
			for (int i=0; i< count; i++) {
				strbuf.append(word + " ");
			}
		} 
		return (strbuf.toString());
	}
	
	public String[] getBibEntryAsArrayOfString() {
		String str= getTitleAsBOW() + " " + getAbstractBOW();
		String[] arr= str.split(" ");
		return arr;
	}
}
