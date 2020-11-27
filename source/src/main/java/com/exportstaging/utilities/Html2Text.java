package com.exportstaging.utilities;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.parser.ParserDelegator;

import com.exportstaging.api.exception.ExportStagingException;

/**
 * For stripping html tags from html
 * 
 * @author himanshu.mishra
 *
 */
public class Html2Text extends HTMLEditorKit.ParserCallback {
    StringBuffer htmltext;
    
    @Override
    public void handleText(char[] text, int pos) {
        htmltext.append(text).append(' ');
    }

    /**
     * 
     * @param html
     * @return
     * @throws ExportStagingException
     */
    public static String stripHtml(String html) throws ExportStagingException {
        Html2Text parser = new Html2Text();
        try (Reader reader = new StringReader(html)){
            parser.parse(reader);
        } catch (IOException e) {
            throw new ExportStagingException(e);
        }
        return parser.getText();
    }

    /**
     * get text available with provided html
     * 
     * @return text
     */
    private String getText() {
        return htmltext.toString().trim();
    }
    
    /**
     * Parser to remove html tags from the content supplied with reader object
     * 
     * @param in reader object
     * @throws IOException
     */
    private void parse(Reader in) throws IOException {
        htmltext = new StringBuffer();
        // the third parameter is TRUE to ignore charset directive
        new ParserDelegator().parse(in, this, Boolean.TRUE);
    }
    
    
}