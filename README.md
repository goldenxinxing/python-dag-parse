
- If local debug, go to test dir, use main.py
- If install to local, 
  - go to current root dir, exec 
    ```bash 
    bash python setup.py install --user
    ```
  - cd to anywhere(such as '/home/**'), and copy test/step.py, exec
    ```bash
    /home/***/.local/bin/swjob --function=evaluate_ppl --module=step --path=/home/**
    ```