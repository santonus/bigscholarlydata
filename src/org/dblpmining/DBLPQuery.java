package org.dblpmining;


import java.io.FileInputStream;
import java.io.IOException;

import javax.xml.namespace.QName;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
public class DBLPQuery {

	private String _dblpFile;
	private XPath xPath;
	private InputSource _inputSource;

	public DBLPQuery(String xmlFile) {
		this._dblpFile = xmlFile;
		initObjects();
	}

	private void initObjects(){        
		try {
			xPath =  XPathFactory.newInstance().newXPath();
			_inputSource = new InputSource(new FileInputStream(_dblpFile));
		} catch (IOException ex) {
			ex.printStackTrace();
		}     
	}

	public String getStringValue(String expression) { 
		try {
			return (String) xPath.evaluate(expression, _inputSource, XPathConstants.STRING);
		} catch (XPathExpressionException ex) {
			ex.printStackTrace();
			return null;
		}
	}
	public Float getNumberValue(String expression) { 
		try {
			return (Float) xPath.evaluate(expression, _inputSource, XPathConstants.NUMBER);
		} catch (XPathExpressionException ex) {
			ex.printStackTrace();
			return null;
		}
	}
	public Node getNode(String expression) { 
		try {
			return (Node) xPath.evaluate(expression, _inputSource, XPathConstants.NODE);
		} catch (XPathExpressionException ex) {
			ex.printStackTrace();
			return null;
		}
	}
	public NodeList getNodeList(String expression) { 
		try {
			return (NodeList) xPath.evaluate(expression, _inputSource, XPathConstants.NODESET);
		} catch (XPathExpressionException ex) {
			ex.printStackTrace();
			return null;
		}
	}
}
