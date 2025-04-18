echo "Deploying Hera templates..."

echo "1. Deploying converg-quader workflow..."
python converg-quader-workflow.py

echo "2. Deploying converg-quaque workflow..."
python converg-quaque-workflow.py

echo "3. Deploying converg-xp workflow..."
python converg-workflow.py 

echo "4. Deploying blazegraph-xp workflow..."
python blazegraph-workflow.py 

echo "5. Deploying db-xp workflow..."
python db-workflow.py 

echo "6. Deploying ds-xp workflow..."
python ds-workflow.py 

echo "7. Deploying ds-dbs-xp workflow..."
python ds_dbs-workflow.py 

echo "8. Deploying workflow-xp-main workflow..."
python main-workflow.py 

echo "Deployment complete."