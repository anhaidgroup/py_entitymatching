cd /Users/pradap/Documents/Research/Python-Package/anhaid/py_entitymatching/docs
make clean html
cd _build/html
scp -r * pradap@trinity.cs.wisc.edu:~/public/html-www/magellan/user_manual/multi_page
cd /Users/pradap/Documents/Research/Python-Package/anhaid/py_entitymatching/docs
make clean singlehtml
cd _build/singlehtml
scp -r * pradap@trinity.cs.wisc.edu:~/public/html-www/magellan/user_manual/single_page
cd /Users/pradap/Documents/Research/Python-Package/anhaid/py_entitymatching/docs
