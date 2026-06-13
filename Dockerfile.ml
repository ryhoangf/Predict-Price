FROM python:3.10-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir \
    fastapi==0.115.12 \
    uvicorn[standard]==0.34.3 \
    pandas==2.2.3 \
    numpy==2.2.6 \
    scipy==1.13.1 \
    scikit-learn==1.7.2 \
    joblib==1.5.3 \
    sqlalchemy==2.0.41 \
    pymysql==1.1.1 \
    flashtext==2.7 \
    python-dotenv==1.1.0
COPY spark_apps/predictprice/api ./api
COPY spark_apps/predictprice/ml_models ./ml_models
COPY spark_apps/predictprice/config ./config
COPY spark_apps/predictprice/NLP/title_nlp.py ./NLP/title_nlp.py
COPY spark_apps/predictprice/models/smart_price_predictor.pkl ./models/smart_price_predictor.pkl
COPY spark_apps/predictprice/models/temporal_price_forecaster.pkl ./models/temporal_price_forecaster.pkl
COPY spark_apps/predictprice/models/depreciation_model.pkl ./models/depreciation_model.pkl
EXPOSE 8000
CMD ["uvicorn", "api.api_depreciation:app", "--host", "0.0.0.0", "--port", "8000"]
