use ExtUtils::MakeMaker;

##----------------------------------------------------------------------
## MAIN
##----------------------------------------------------------------------
WriteMakefile
  (
   NAME		=> 'DiaColloDB',
   AUTHOR       => 'Bryan Jurish <moocow@cpan.org>',
   ABSTRACT	=> 'Diachronic collocation index',
   VERSION_FROM	=> 'DiaColloDB.pm',

   PREREQ_PM    => {
		    'DDC'           => 0.16,
		    'DDC::XS'       => 0.04,
		    'File::Path'    => undef,  ##-- make_tree, remove_path; 2.08_01
		    'File::Map'     => undef,  # 0.63
		    'JSON'          => undef,  # 2.53
		    'IPC::Run'      => undef,  # 0.94
		    'Log::Log4perl' => 1.07,   # 1.29
		   },


   EXE_FILES   => [ glob('dcdb-*.perl'), ],
   NORECURS => 1, ##-- don't recurse into local 'MUDL' mirror dir
  );
