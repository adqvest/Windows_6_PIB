import camelot

cam = camelot.read_pdf(r"C:\Users\Administrator\AdQvestDir\codes\One time run\udise_mum.pdf", flavor='stream', pages = 'all')

d = cam[0].df


print(len(d))

for i in cam:
	print(i.df)