    README for DiaColloDB

ABSTRACT
    DiaColloDB - diachronic collocation database

REQUIREMENTS
    DB_File
        For handling large temporary hashes during index construction.

    DDC::Concordance (formerly ddc-perl)
        Perl module for DDC client connections. Available from
        https://sourceforge.net/projects/ddc-concordance/files/ddc-perl/

    DDC::XS (formerly ddc-perl-xs)
        XS wrappers for DDC query parsing. Available from
        https://sourceforge.net/projects/ddc-concordance/files/ddc-perl-xs/

    File::Map
    File::Path
    File::Temp
    JSON
    IPC::Run
    Log::Log4perl
    PDL Perl Data Language for fast fixed-size numeric data structures, used
        by the TDF (term-document frequency matrix) relation type.

    PDL::CCS
        PDL module for sparse index-encoded matrices, used by the TDF
        (term-document frequency matrix) relation type.

    Tie::File::Indexed
        For handling large (temporary) arrays during index creation.

    (a corpus to index or an existing index to query)

DESCRIPTION
    The DiaColloDB package provides a set of object-oriented Perl modules
    and a command-line utility suite for constructing and querying native
    diachronic collocation indices with optional inclusion of a DDC server
    back-end for fine-grained queries.

INSTALLATION
    Issue the following commands to the shell:

     bash$ cd DiaColloDB-0.01 # (or wherever you unpacked this distribution)
     bash$ perl Makefile.PL   # check requirements, etc.
     bash$ make               # build the module
     bash$ make test          # (optional): test module before installing
     bash$ make install       # install the module on your system

SEE ALSO
    *   http://kaskade.dwds.de/dstar/dta/diacollo/ contains a live web demo
        of a DiaCollo index on the *Deutsches Textarchiv* corpus, including
        a user-oriented help page.

AUTHOR
    Bryan Jurish <moocow@cpan.org>

