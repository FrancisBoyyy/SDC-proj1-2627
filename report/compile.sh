#!/usr/bin/env bash

NAME="$1"

set -e

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <name of the text file (without .tex)>"
    exit 1
fi

if [ ! -f "$NAME.tex" ]; then
    echo "Error: The file '$NAME.tex' does not exist."
    exit 2
fi

# clean old files like logs, and auxiliaries
rm -f "/$NAME".{aux,bbl,blg,log,out}

# compile process
latex acmart.ins

pdflatex "$NAME.tex"

bibtex "$NAME"

pdflatex "$NAME.tex"
pdflatex "$NAME.tex"

echo "PDF generated: $NAME.pdf !"

