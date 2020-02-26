# ISCE2GIAnT
Adapting ISCE products ready to run in GIAnT

Three steps necessary for this running

    1.  topsApp_inTurn_1_prep_IFG.py    this is to call topsApp.py up to compute baselines used
                                            to choose IFGs.

    2.  topsApp_inTurn_2_choose_IFG.py  this is to choose IFGs to run in detail in step 3

    3.  topsApp_inTurn_3_run_IFG.py     this is to run topsApp.py in full for IFGs chosen in
                                            step 2.
