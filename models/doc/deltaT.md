# DeltaT

**deltaT** is a range in time after the creation of a dataset in which we
assume the dataset will stay in disk regardless whether it has been accessed or not
and/or the deletion policy in place.It will be calculated as the typical time
between the creation(d_creation_date) or modification(d_last_modification) date
of a dataset and its first access. Following, the procedure to define **deltaT**
is described.

 

