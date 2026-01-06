# Python code to illustrate the Modules
import re




def full_clean(text):
    """Clean the text by replacing unwanted characters ('#', '@', '$', '%', '^', '&', '.', '!', '*', ';', ':', "'", '"', '?', '-', '(', ')', '[', ']', '{', '}', '\', '~', '`', '/', '_', '+', '=', ',', '|', '<', '>')



       Args:
       text : pass a string

       Output:
       Return a clean string by removing all unwanted characters"""
    #https://stackoverflow.com/questions/4278313/python-how-to-cut-off-sequences-of-more-than-2-equal-characters-in-a-string?fbclid=IwAR397HRMq3BILIL5VIs3kFBPf_KF7Z0siH2FG3MubLYPkOXIdcbVAaGj_Is
    #https://stackoverflow.com/questions/4278313/python-how-to-cut-off-sequences-of-more-than-2-equal-characters-in-a-string?fbclid=IwAR397HRMq3BILIL5VIs3kFBPf_KF7Z0siH2FG3MubLYPkOXIdcbVAaGj_Is

    #text = text.lower() # convert text to lower case string or lower case text

    #text = re.sub(r'((www\.[S]+)|(https?://[\S]+))','URL',text)# replcae the website link to only "URL"

    text = re.sub(r'#',' ',text) # Removing hash(#) symbol .
    text = re.sub(r'@',' ',text) # replace @ with space
    text = re.sub(r'\$',' ',text) # replace $ with space
    text = re.sub(r'%',' ',text) # replace % with space
    text = re.sub(r'\^',' ',text) # replace ^ with space
    text = re.sub(r'&',' ',text) # replace & with space

    text = re.sub(r'\.+', ' ', text) # Replacing one or more dot to single space

    text = re.sub(r'!+',' ',text) # Replacing single or more than one explanation symbol to single space
    text = re.sub(r'\*+',' ',text) # Replacing one or more star(*) symbol to single space
    text = re.sub(r';+',' ',text)   # Replacing  single semicolor(;)  or more semicolon(;) symbol to single space
    text = re.sub(r':+',' ',text)   # Replacing single colon(:) or more colon(:) sybol to single space
    text = re.sub(r"\'+",' ', text) # Replacing single quote or more single quote to single space

    text = re.sub(r'\"+',' ', text) # Replacing double quote or more double  quote to single space
    text = re.sub(r'\?+',' ', text) # Replacing single question mark or more question mark to single space
    text = re.sub(r'-+', '', text) # Replacing single hypen  symbol or more hypen symbol  to single space
    text = re.sub(r'[\(\)]+',' ', text) # Replacing left paranthesis or right paraenthesis to single space
    text = re.sub(r'[\[\]]+',' ', text) # replacing [ and ]
    text = re.sub(r'[\{\}]+',' ', text) #replace { and } with space
    text = re.sub(r'[\\]+',' ', text) # replace \ with space
    text = re.sub(r'[~`]+',' ', text) # replace ~ and ` with space
    text = re.sub(r'[/]+',' ', text) # replace / with space
    text = re.sub(r'[_+=,]+',' ', text) # replace _ + = and , with space
    text = re.sub(r'[|]+',' ', text) # replace | with space
    text = re.sub(r'[<>]+',' ', text) # replace < and > with space

    #text = ReplaceThreeOrMore(text) # call the function ReplaceThreeOrMore(text)
    text = re.sub(r'  +',' ',text).strip() # Replacing two or more white space to single white space

    return text  # return the text
