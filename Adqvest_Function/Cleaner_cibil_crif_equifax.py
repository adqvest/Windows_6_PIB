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
    text = re.sub(r'\?+',' ', text) # Replacing single question mark or more question mark to single space--Santonu
    text = re.sub(r'-+', ' ', text) # Replacing single hypen  symbol or more hypen symbol  to single space--Santonu
    text = re.sub(r' – +', ' ', text) # Replacing single hypen  symbol or more hypen symbol  to single space
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


def clean_company(text,new_mapping={}):
    text = text.lower()
    text = text.replace(',',' ')
    text = text.replace('|',' ').replace('*','').replace(']','').replace('[','').replace("`",' ')
    text = re.sub(r'#',' ',text) # Removing hash(#) symbol .
    text = re.sub(r'@',' ',text) # replace @ with space
    text = re.sub(r'\$',' ',text) # replace $ with space
    text = re.sub(r'%',' ',text) # replace % with space
    text = re.sub(r'\^',' ',text) # replace ^ with space
    #text = re.sub(r'&',' ',text) # replace & with space
    text = re.sub(r'\d+ml', ' ', text)
    text = re.sub(r'\((.*?)\)', ' ', text)
    text = re.sub(r'[-]', ' ', text)
    text = re.sub(r'  +', ' ', text).strip()
    text = re.sub(r'\d+ ml', ' ', text)
    text = re.sub(r'\d+', ' ', text)
    text = re.sub(r"'\w+", " ", text)
    text = re.sub(r'  +', ' ', text).strip()
    text = text.replace('-','').replace('(','').replace(')','').replace('\n','').replace('.','')
    text = text.replace("'",' ').replace(';','').replace('"','')
    text = text.replace('/','').replace('+','')
    text = re.sub(r'[?|$|.|!]',r'',text)
    text = text.replace('the','').replace('&',' and ')

    def Merge(dict1, dict2):
        res = {**dict1, **dict2}
        return res
    mapping = {'limited':'ltd','private':'pvt','company':'co','cooperative':'coop','corporation':'corp',
            'technology':'tech','international':'intl','engineering':'engg','solutions':'soln','solution':'soln',
            'privatelimited':'pvt ltd','bnak':'bank','corpn':' corp','housing development finance corp':'hdfc'}
    mapping = Merge(mapping, new_mapping)
    for i in mapping.items():
        text = re.sub(r'(?<!\S)' + i[0] + '+(?!\S)', i[1], text, flags=re.IGNORECASE)
    text = text.replace('  ',' ')
    text = re.sub(r'  +',' ', text).strip()

    return text


def clean_location(text):
    text = text.lower()
    text = text.replace(',',' ')
    text = text.replace('|',' ').replace('*','').replace(']','').replace('[','').replace("`",' ')
    text = re.sub(r'#',' ',text) # Removing hash(#) symbol .
    text = re.sub(r'@',' ',text) # replace @ with space
    text = re.sub(r'\$',' ',text) # replace $ with space
    text = re.sub(r'%',' ',text) # replace % with space
    text = re.sub(r'\^',' ',text) # replace ^ with space
    text = re.sub(r'&',' ',text) # replace & with space
    text = re.sub(r'\d+ml', ' ', text)
    text = re.sub(r'\((.*?)\)', ' ', text)
    text = re.sub(r'[-]', ' ', text)
    words = ['rto','apmc','sdm','arto','dto','rla','srto','uo','rta','sta','apmcs','krishi upaj mandi samiti','krishi upaj samiti']
    for w in words:
      text = re.sub(r'(?<!\S)' + w + '+(?!\S)', "", text, flags=re.IGNORECASE)
    text = text.replace('-','').replace('(','').replace(')','').replace('\n','').replace('.','')
    text = text.replace("'",' ').replace(';','').replace('"','')
    text = text.replace('/','').replace('+','')
    text = re.sub(r'[?|$|.|!]',r'',text)
    text = text.replace('  ',' ')
    text = re.sub(r'  +', ' ', text).strip()
    text = re.sub(r'\d+', ' ', text)
    text = re.sub(r"'\w+", " ", text)
    text = re.sub(r'  +', ' ', text).strip()

    return text


def clean_bnk_br_st_ad(x):
  bank_mapping={
      'Â':'','Ã':'','¶':'','LTD':'','‚':'','€“ii':'','€“':'','¢¬€œ':'',' BOM ':'BANK OF MAHARASHTRA','SYNDICATEBANK':'SYNDICATE BANK','CITIBANK NA':'CITI BANK',
      'BANK AG':'BANK','HDFC BANK':'HDFC BANK','BANK NA':'BANK','UCOBANK':'UCO BANK','PVT':'','DEHRA DUN':'DEHRADUN'}
  
  short_forms={
      'ASSET':['AASHSMETE','AASET','A IISSETS','ASSDEETL','SSET','A IISSETS ','ASSEVTE','ACSOSIEMTB','AANSCSEHT','ASDSEETL','ASSETS','AANSSCEHT','A SNHE', 'A SNHE WII', 'A SNSEEWT', 'A SNSEEWT', 'A SSET', 'A SSET ', 
               'A SSET ', 'A SSRUUORRDAAATT', 'A ST', 'AAHSSAETTIS','AAHSSAETTIS', 'AAGSPSEUTR', 'AASNSCETH','AASNSCETH M', 'AASNSCETH M ', 'AASNSEATG', 'AASNSEATG ','AASSSSEETT','AASSSSEETT ', 'AASSSSEETT  ', 'AASUSREATN','AASUSREATN ', 'AASUSREATN ', 
               'AASUSREATN', 'AASUSREATN', 'AMSASDETU', 'AMSASDETU', 'AMSESNETT','AMSESNETT', 'AMSSUEMT','AMSSUEMT BMAAINAGEMENT', 'AMSSUEMT', 'AMSSUETM','AMSSUETM ', 'AMSUSEMTB',
               'ASSTS','ANSCSHE','ACSHS EATHSM','AASNSCEHT','ANSCSHE TH','AANSCSHET N','ARSASNETC','ARSASNECTH','ASSETISI'],
              
      'RECOVERY':['RBERCAONVCEHRY','RAEHCMOEVDEARYB','RAEHCMOEVDEBRANCHY','RBERCAONVCEHR','RBEACNOGVAELROY','RGEACBOAVDE','RGEACBOAVDE R','RREACIOVERY','RCAECHONVDEIGRYA','RBERCAONVCEHRY','ERMEECNOTVERY','RREECCOOVVEERRYY','RREECCOOVVEERRYY','RGAEBCOAVDERY','RGAEBCOAVDERY','RAEHCMOEVDEBRANCHY','RAEHCMOEVDERAYB','RAEHCMOVEDERAYB','RAHECMOEVDEBRANCHY',
                  'RBEACNOGVAERLOY','RCEOCIOMVBEARTYO','RECCOHEVNERNYA','RECONVEORIDY','RECOVERMY',
                  'RECROANVECRHYI','REJACBOAVLEPRUYR','REKCOOLVKEARTYA','RELCUOCVKENROYW','REMCOAVDEURRYA','RENCOAGVPERUYR','RHEYCDOEVREBRANCHYA','RJEACBOAVLEPRUYR',
                  'RECOVERYC','RECOVERYR','RECROAVNECRHYI','RKECOOLKVAETRTYA','RHEYCDOEVREBRANCHYA','RENCOAVGEPRUYR','RELUCOCKVNEROYW','RAEHCMOEVDEBRANCHY','REBCROANVECRHY',
                  'RECOVERYK','RECOVERYM','REJCAOBAVLEPRUYR','REKCOOLVKEARTYT','RELCUOCKVNEROYW','REMCAODVUERRAY','RHECYDOEVREBRANCHYA','RHEYCDOEVREBRANCHYA',
                  'RECOVERYK','RECOVERYM','REJCAOBAVLEPRUYR','REKCOOLVKEARTYT','RELCUOCKVNEROYW','REMCAODVUERRAY','REMCOUMVEBRAYI','REMCOUVMERBYA',
                  'RECOVERYK','RECOVERYM','REJCAOBAVLEPRUYR','REKCOOLVKEARTYT','RELCUOCKVNEROYW','REMCAODVUERRAY','REMCOUMVEBRAYI','REMCOUVMERBYA',
                  'RHECYDOEVREBRANCHYA','RHEYCDOEVREBRANCHYA','RHEYCDOEVREBRANCHY',' REC ','RHECYDOEVREBRANCHYA','RHEYCDOEVREBRANCHY','RHEYCDOEVREBRANCHYA','RRTEICCOAVL ERY',
                  'RAEHCMOEVDEBRANCHY','RAHECMOEVDEARYB','RAHECMOEVDEBRANCHY','RBEACNOGVAERLYO','RCEACHONVDEIRGYA','REBCROAVNECRHY','RECCHOEVNENRAY',
                  'RECCHOEVNENRAYI','RECCHOEVNENRYA','RECNOAVGEPRUYR','RECOBVERRAYN','RECOVBERRAY','RECOVDEREYL','RECOVDEREYLH','RECOVEBRRYA','R EMCOUMVEBRAYI','DRAEBCAODV E R Y','DRAEBCOADVERY',
                  'RECOVERBYR','RECOVERNYE','RHECYDOEVREBRANCHYA','RHECYDOEVREBRANCHYA','RIEGCAORVHE R Y','RAEI COVERY','RAEIC O V E RY','RAEICOVERY','RAETCOORVEERY','RAETCOOVERY',
                  'ARUECRAONVEGRAYB','ARUECROANVGERAYB','BRREACNOCVHE','BRREACNOCVHER','CRAEHCONVDEIGRYA','CROECIMOBVAERTOY','RAEHCMOEVDEBRANCHY',
                  'RAEUCROAVNEGRAYB','RAHECMOEVDEBRANCHY','RAUECROAVNEGRAYB','RBEACONVGAERLYO','RBECRAONVECHRY','RBECROANVCERHY','RECMOAVDEURRYA',
                  'RECOVDERELYH','RECOVEBRRYA','RECOVERBYR','RAEHCMOEVDEBRANCHY','RAHECMOEVDEBRANCHY','RBEACONVGEARLYO','RBECROANVECRHY','RECCOHVEENRNYA','RECOVEBRYA'],
                                              
      'MANAGEMENT':['MDEALNHAIGEMENT','SK OMLAKNAATGAEMENT','MUMANBAAGIEMENT','HMIANAGEMENT','MADNEALGHEIMENT','M G M N T ','R MAANNCHAGIEMENT','NMANAGEMENT','M MANAGEMENT','MANACNHAIGEMENT','R MAANNCHAIGEMENT','NMGAALNOAREG E MENT','NMGAANLOARGEE MENT','OMLAKNAATAGEMENT','MUAMNBAAGIEMENT','MANACHGEEMNNEANIT','MANAGEMENTBRANCH','MAUNMAGBEAMI','MANKAOGLEKMATEANT',
                    'MANCAHGEENMNEANIT','MANCAHGEENMNEANIT','MANKAOGLEKMATEANT','MBAAINAGEMENT','HMEAENNAAGI EMENT','MHEANNNAAGIEMENT','RMAANNCAHGIEMENT','UMMANBAAGIEMET','RMAANNCHAIGEMENT',
                    'MEADNAABGAEDMENT','M MANAGEMENT ENT','MYDAENRAAGBEAMDENT','MEWAN DAEGLEHMIENT','YMDAENRAAGBEAMDENT','MIGIMNT','MGMNT','MANAGEMENTBR',
                    'M MAUNMAGBAEMI ENT','BMAAINAGEMENT','YMDAENRAAGBEAMDENT','N MEWAN DAGELEHMIENT','H MYDAENRAAGBEAMDENT','TH YMDAENRAAGBEAMDENT','N MEWAN DAEGLEHMIENT'],
      'BRANCH':['BBRRAANNCCHH','BRANCYH','B RA N C H','B R A N C H','R Y B RANCH','RBERANCH','RBHRA N C H','B R A N CH','BDR A N C H','BDRANCH','ABDR A N C H'],
      'ALIGARH':['A LI G A R H'],
      'STRESSED':['STREBSRSA','STRESBSREADN','STRESBSR','STREBSRS','STRESSEDM','STRESSEDBR','STRESSEDB','STRESSEBDR'],
      'COLLEGE':['C O L LE G E'],
      'NETRWORK BRANCH':['NETRWORKBRANCH'],
      'VS':['V S '],
      'VN':['V N '],
      'DELHI ':['D EL H I '],
      'IA ':['I A '],
      'IBD ':['I B D '],
      'IE ':['I E '],
      'KG ':['K G '],
      'KB ':['K B '],
      'KC ':['K C '],
      'KEM ':['K E M '],
      'SAA ':['S A A '],
      'SCF ':['S C F '],
      'SME ':['S M E '],
      'SM ':['S M '],
      'SN ':['S N '],
      'SSI ':['S S I'],
      'AB ROAD':['A B ROAD','A B RODA'],
      'MG ':['M G '],
      'MI ':['M I '],
      'MJ ':['M J '],
      'MUMBAI ':['M U M B A I '],
      'NAGAR ':['N A G A R '],
      'NM ':['N M '],
      'NG ':['N G '],
      'ROAD ':['R O A D'],
      'AVENUE ':['A V EN U E ']}

  text=clean_company(full_clean(x)).upper().strip()
  for k,v in bank_mapping.items():
      text=text.replace(k,v)
  
  for k,v in short_forms.items():
      for j in  short_forms[k]:
          text=text.replace(j,k)
  
  short_form_2={
                'ARMB ':['A R M B ','ASSET RECOVERY MANAGEMET BRANCH ','A M BRANCH ','ASSET RECOVERYANAGEMET BRANCH ','A R M BRANCH','ASSET RECOVERY MANAGEMENT ','ASSET RECOVERY ','ARMB BRANCH','A M BRANCH','A R M BRANCH','A R M'],
                'LCB':['L C B'],
                'ADB':['A D B'],
                'SAMB':['S A R B','S A R B','S A R C','S ARMB','SAM BRANCH','SAM BR','SAM B','STRESSED ASSETS MANAGEMENT BRANCH','STRESSED ASSET MANAGEMENT BRANCH','STRESSED ARMB','STRESSED ASSET MANAGEMENT','S A M B','STRESS ASSET MANAGEMENT BRANCH'],
                'IFB':['I F B']}
  
  for k1,v1 in short_form_2.items():
      for j1 in  short_form_2[k1]:
          # print(j1)
          text=text.replace(j1,k1)
          
  return text.strip()