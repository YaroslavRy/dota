## Dota 2 pro matches parsing
### With python 3 and MongoDB
- pro_matches_id_loader notebook is for obtaining id of pro matches from the API opendota
- pro_matches_loader serves for obtaining full matches data by id from the API opendota

Here I use proxy. Function `get_working_proxy` automatically finds working proxy address. Function is called when the limit is exceeded.
