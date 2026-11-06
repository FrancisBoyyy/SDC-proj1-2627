# How to build the pdf report from LATEX

This file presents instructions on how to build a pdf file from scratch using LATEX and the libraries provided by the teacher.

## 1. Installation

If you haven't LATEX on your computer, please intall it with:

```bash
sudo apt install texlive-full
```

this provides the dependencies from `pdflatex`, `bibtex` to produce the documents, and all needed libraries.

*NOTE: please, do not install the `textlive-basic` package since it does not include the libraries the teacher uses in its template.*

The installation process should be about 30 minutes, because it fully downlaods the compiler and libraries.

## 2. Compilation

To compile the `.tex` file, run the following bash script with the argument being simply the name of the file (without the extension):
```bash
cd report
chmod +x compile.sh
./compile.sh <name of the text file (without .tex)>
```
This will generate the desired `.pdf` file, with the correspondent bibliographic references.

## 3. References / Citations

So, you have the file with the extension `.bib` that has all the BibTEX references, you just need to insert the reference from the cited paper there, and LATEX will recognize that in the `.tex` file.


