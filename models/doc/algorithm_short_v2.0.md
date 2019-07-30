# Algorithm V2.0

After observing the results of the first version of the algorithm, it was decided
that it is necessary to make a distinction between datasets that are being recalled
and those which are accessed for first time. 

The current algorithm considers a dataset that is accessed for the first
time as recalled which is a wrong assumption given that newly created datasets
are written into disk.

In order to distinguish bewteen recalled and newly created datasets
we need to know the date in which they were created and compare that with the
access date.

In the 'dataset' table of the DBS database there
are two fields that we could use to define the creation date of a dataset:
**d_creation_date** and **d_last_modification_date** but there are few
considerations that we need to keep in mind first:

 * Not all the datasets are created the same
 * Some are let open for a while (more files are added later)
 * Simulation get open and close faster than detector data 

The above considerations tell us that knowing when a dataset has been created cannot be defined
by a simple date. For that reason we have decided to define a range of time in
wich we will consider a dataset as "newly created" and hence in disk.
This range of time know as **deltaT** will be the typical time between
the creation(d_creation_date) or modification(d_last_modification) date of a
dataset and its first access.The details on how **deltaT** is calculated can be found in
the section: [DeltaT](deltaT.md)

Once we know the typical **deltaT** we will consider a newly created dataset to
be on disk for the period of **policy** + **deltaT**. For example, if we define
a deletion policy of three weeks and deltaT is one week, then we will consider
a newly created dataset to remain in disk after 4 weeks of not being accessed


