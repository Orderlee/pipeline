import tensorrt as trt
import torch
from .trt_utils import trt_to_torch_dtype, run_v3
from torchvision.io import read_image, ImageReadMode

class TRTInference:
    def __init__(self, engine_path):
        self.logger = trt.Logger(trt.Logger.ERROR)
        with open(engine_path, "rb") as f, trt.Runtime(self.logger) as runtime:
            self.engine = runtime.deserialize_cuda_engine(f.read())
        self.context = self.engine.create_execution_context()
        self.__init_values__()

    def __init_values__(self):
        self.io_tensor_num = self.engine.num_io_tensors
        self.tensor_names = [self.engine.get_tensor_name(i) for i in range(self.io_tensor_num)]
        self.inp_names = [
            n for n in self.tensor_names if self.engine.get_tensor_mode(n) == trt.TensorIOMode.INPUT
        ]
        self.out_names = [
            n
            for n in self.tensor_names
            if self.engine.get_tensor_mode(n) == trt.TensorIOMode.OUTPUT
        ]
        self.trt_input_dtypes = [self.engine.get_tensor_dtype(name) for name in self.inp_names]
        self.torch_input_dtypes = [trt_to_torch_dtype(dt) for dt in self.trt_input_dtypes]
        self.trt_output_dtypes = [self.engine.get_tensor_dtype(name) for name in self.out_names]
        self.torch_output_dtypes = [trt_to_torch_dtype(dt) for dt in self.trt_output_dtypes]
        self.out_shape = tuple(
            (1, *self.context.get_tensor_shape(self.out_names[0])[1:])
        )  # 초기 output dim을 고정하여 동작하도록
        self.outputs = {
            name: torch.empty(self.out_shape, dtype=self.torch_output_dtypes[i], device="cuda")
            for i, name in enumerate(self.out_names)
        }

    def __call__(self, *args, **kwds):
        return self.infer(*args, **kwds)

    def infer(self, image_cuda):
        # Preprocess the image
        for input_name in self.inp_names:
            self.context.set_input_shape(input_name, tuple(image_cuda.shape))
        for output_name in self.out_names:
            dims = self.context.get_tensor_shape(output_name)
            out_shape = tuple([int(image_cuda.shape[0])] + [d for d in dims[1:]])
            if self.out_shape != out_shape:
                self.outputs = {
                    name: torch.empty(out_shape, dtype=self.torch_output_dtypes[i], device="cuda")
                    for i, name in enumerate(self.out_names)
                }
                self.out_shape = out_shape
        bindings = [0] * self.io_tensor_num
        bindings[self.tensor_names.index(input_name)] = int(image_cuda.data_ptr())
        for name in self.out_names:
            bindings[self.tensor_names.index(name)] = int(self.outputs[name].data_ptr())
        # 컨텍스트에 주소 등록
        for i in range(self.io_tensor_num):
            name = self.engine.get_tensor_name(i)
            self.context.set_tensor_address(name, bindings[i])
        ok = run_v3(self.context)
        if not ok:
            raise RuntimeError("TensorRT inference failed.")
        if len(self.out_names) == 1:
            return self.outputs[self.out_names[0]]
        return self.outputs

if __name__ == "__main__":
    engine_path = "./trt/PE-Core-L14-336-Vision.engine"
    image_path = "assets/dog.jpg"
    image = read_image(image_path, mode=ImageReadMode.RGB)
    if image is None:
        raise ValueError(f"Image not found at {image_path}")

    trt_inference = TRTInference(engine_path)
    ret = trt_inference.infer(image)

    print(f"TRT Inference Result: {ret.shape}")
    print(ret)
