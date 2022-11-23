
import numpy as np
from flask import Flask, render_template,request
import pickle

model = pickle.load(open("model.pkl",'rb'))
app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/predict',methods=['POST'])
def predict_drybeen():
    area = float(request.form.get("area"))
    perimeter = float(request.form.get("perimeter"))
    majot_axis_length = float(request.form.get('majot_axis_length'))
    minor_axis_length = float(request.form.get("minor_axis_length"))
    aspect_ratio = float(request.form.get("aspect_ratio"))
    eccentricity = float(request.form.get('eccentricity'))
    convex_area = float(request.form.get("convex_area"))
    equivalent_diameter = float(request.form.get("equivalent_diameter"))
    extent = float(request.form.get('extent'))
    solidity = float(request.form.get("solidity"))
    roundness = float(request.form.get("roundness"))
    compactness = float(request.form.get('compactness'))
    sf1 = float(request.form.get("sf1"))
    sf2 = float(request.form.get("sf2"))
    sf3 = float(request.form.get("sf3"))
    sf4 = float(request.form.get("sf4"))


    # prediction
    result = model.predict(np.array([area, perimeter, majot_axis_length,minor_axis_length,aspect_ratio
                                     ,eccentricity,convex_area,equivalent_diameter,extent,solidity,roundness
                                     ,compactness,sf1,sf2,sf3,sf4]).reshape(1,16))

    return render_template('index.html', result=result)


if __name__ == '__main__':
    app.run(debug=True)
    # app.run(host = '0.0.0.0', port=8080)


